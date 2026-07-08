"""연결별 fan-out 라우터 (#3).

공유 consumer(`messaging.consumer`) 하나가 `stock.public.stock_trades` 전체를
읽고, 파싱된 `CdcTradeEvent` 배치를 이 라우터가 **ticker 별 구독자 큐**로 분배한다.

**핵심 설계 결정**(개발 항목 #3): 연결마다 전용 consumer 를 두면 1,000 동시연결 =
1,000 consumer 로 브로커 부담(그룹 멤버·리밸런싱·TCP)이 폭증한다(성능요구 #1).
그래서 '공유 consumer 1개 + 프로세스 내 fan-out 라우팅'을 택했다 — 이벤트당 비용은
`O(해당 ticker 구독자 수)` 이고 브로커는 관여하지 않는다.

**의존 방향**: consumer 는 이 모듈을 임포트하지 않는다. main.py(조립 지점)가
`stock_trade_consumer.set_event_handler(ticker_router.route)` 로 messaging → realtime
방향을 연결하므로 순환 임포트가 생기지 않는다(messaging → realtime.model 단방향 유지).
"""

import asyncio
from collections import defaultdict

from utils.logger import logger_instance

from .model import CdcTradeEvent

logger = logger_instance()

# 구독자별 이벤트 버퍼 상한. 느린 WS 소비자가 있어도 라우터(=consumer 루프)가
# 절대 블로킹되지 않도록 유한 큐를 쓴다. 큐가 가득 차면 가장 오래된 이벤트를
# 버리고 최신을 유지한다(실시간 시세는 최신성 우선). 정교한 backpressure/conflation
# 정책은 #19 에서 다룬다 — 여기서는 '라우터는 결코 막히지 않는다'만 보장한다.
_SUBSCRIBER_QUEUE_MAXSIZE = 1000


class Subscription:
    """단일 WS 연결이 특정 ticker 이벤트를 받아가는 핸들.

    라우터가 `route()` 에서 `_offer()`(논블로킹)로 이벤트를 넣고, 연결의 stream
    태스크가 `get()`(await)으로 꺼낸다. 하나의 연결 상태(#3)가 소유하는 '구독 핸들'
    이며, ticker 전환 시 새 인스턴스로 교체된다(#5 는 이 위에 전환 정책을 얹는다).
    """

    __slots__ = ("ticker", "_queue", "_dropped")

    def __init__(self, ticker: str, maxsize: int) -> None:
        self.ticker: str = ticker
        self._queue: asyncio.Queue[CdcTradeEvent] = asyncio.Queue(maxsize=maxsize)
        # 큐 초과로 버려진 이벤트 누계(운영 가시성 #18 의 기초).
        self._dropped: int = 0

    def _offer(self, event: CdcTradeEvent) -> None:
        """라우터 전용: 논블로킹으로 이벤트를 넣는다(가득 차면 최신 유지).

        `put_nowait` 이 실패하면 가장 오래된 항목을 버리고 최신을 넣어, 느린
        소비자 때문에 이벤트 루프(소비 루프)가 멈추지 않게 한다.
        """
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            self._dropped += 1
            try:
                self._queue.put_nowait(event)
            except asyncio.QueueFull:
                pass

    async def get(self) -> CdcTradeEvent:
        """다음 이벤트를 기다렸다가 반환한다(연결의 stream 태스크에서 호출)."""
        return await self._queue.get()

    @property
    def dropped(self) -> int:
        """이 구독에서 버퍼 초과로 드롭된 이벤트 수."""
        return self._dropped


class TickerRouter:
    """ticker → 구독자 집합 매핑으로 이벤트를 fan-out 하는 공유 라우터.

    구독/해제/라우팅은 모두 **동기**다. asyncio 협력 스케줄링 하에서 중간에
    await 이 없으므로, 라우팅 도중 구독 집합이 바뀌는 경쟁 조건이 원천적으로
    없다(연결 상태 갱신도 동기 경로로 반영 — `TickStreamer._apply_update` 참고).
    """

    def __init__(self, queue_maxsize: int = _SUBSCRIBER_QUEUE_MAXSIZE) -> None:
        self._subscribers: dict[str, set[Subscription]] = defaultdict(set)
        self._queue_maxsize: int = queue_maxsize

    def subscribe(self, ticker: str) -> Subscription:
        """해당 ticker 의 새 구독을 만든다(연결 시작/ticker 전환 시 호출)."""
        subscription = Subscription(ticker, self._queue_maxsize)
        self._subscribers[ticker].add(subscription)
        return subscription

    def unsubscribe(self, subscription: Subscription) -> None:
        """구독을 해제한다(연결 종료/ticker 전환 시). 중복 호출은 무해하다."""
        subscribers = self._subscribers.get(subscription.ticker)
        if subscribers is None:
            return
        subscribers.discard(subscription)
        # 구독자가 사라진 ticker 키는 정리해 매핑이 무한정 커지지 않게 한다.
        if not subscribers:
            self._subscribers.pop(subscription.ticker, None)

    def route(self, events: list[CdcTradeEvent]) -> None:
        """파싱된 이벤트 배치를 ticker 별 구독자에게 분배한다.

        consumer 루프(이벤트 루프 스레드)에서 직접 호출되므로 **동기·논블로킹**
        이어야 한다 — 여기서 블로킹하면 전체 소비가 멈춘다. 구독자가 없는 ticker
        이벤트는 그냥 버린다(아무도 보고 있지 않은 데이터).
        """
        for event in events:
            subscribers = self._subscribers.get(event.ticker)
            if not subscribers:
                continue
            for subscription in subscribers:
                subscription._offer(event)


# 앱 전역에서 공유하는 단일 라우터(모듈 싱글턴). main.py 가 consumer 와 연결한다.
ticker_router = TickerRouter()
