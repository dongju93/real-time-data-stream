"""단일 WS 연결의 실시간 tick 스트림과 연결별 상태 관리 (#3).

각 WS 연결은 자신의 상태 `{ticker, tick, 구독 핸들, 진행 중 캔들 버킷,
마지막 전송 시각}` 을 독립적으로 소유한다. 데이터는 더 이상 DB 폴링이 아니라
공유 consumer 가 채우는 `ticker_router` 구독에서 온다(fan-out). 서로 다른 ticker
를 요청한 두 연결은 각자 자기 구독의 이벤트만 받으므로 올바른 데이터만 수신한다.

집계는 tick 윈도우 안에서 수신한 체결 이벤트의 open/high/low/close/volume 을
계산한다(#4). ticker 전환 시 기존 구독을 해제하고 새 구독으로 전환하며(#5),
데이터 부재 시 직전 종가 유지 정책은 #6 에서 확장한다.
"""

import asyncio

from fastapi import WebSocket
from pydantic import ValidationError

from utils.logger import logger_instance

from .model import RealtimeTickUpdate, TickData, TradeHighAndLow
from .router import Subscription, ticker_router

logger = logger_instance()

# 갱신 요청이 없을 때 receive_json 이 무한 대기하지 않도록 하는 상한(초).
# 타임아웃마다 루프가 깨어나 취소 신호에 응답한다.
_TICK_UPDATE_TIMEOUT_SECONDS = 60.0


class _TickBucket:
    """진행 중인 tick 윈도우의 집계 상태.

    이벤트가 수신된 순서대로 open(첫 가격), close(마지막 가격), high/low,
    volume 합계를 누적한다.
    """

    __slots__ = ("open_price", "high", "low", "close_price", "volume", "count")

    def __init__(self) -> None:
        self.open_price: float | None = None
        self.high: float | None = None
        self.low: float | None = None
        self.close_price: float | None = None
        self.volume: int = 0
        self.count: int = 0

    def add(self, price: float, volume: int) -> None:
        if self.open_price is None:
            self.open_price = price
        if self.high is None or price > self.high:
            self.high = price
        if self.low is None or price < self.low:
            self.low = price
        self.close_price = price
        self.volume += volume
        self.count += 1

    def reset(self) -> None:
        self.open_price = None
        self.high = None
        self.low = None
        self.close_price = None
        self.volume = 0
        self.count = 0


class TickStreamer:
    """단일 WS 연결의 실시간 tick 스트림 + 연결별 상태 소유(#3).

    두 개의 동시 태스크가 이 인스턴스를 공유한다:
    - `listen_for_tick_updates()`: 클라이언트의 ticker/tick 갱신 수신
    - `stream_data()`: 구독 이벤트를 tick 윈도우로 묶어 캔들 전송

    상태 갱신(`_apply_update`)은 동기 메서드라 asyncio 협력 스케줄링 하에서
    원자적으로 실행되므로 두 태스크 간 경쟁 조건이 없다.
    """

    def __init__(self, ticker: str, tick: int, websocket: WebSocket) -> None:
        self.ticker: str = ticker
        self.tick: int = tick
        self.websocket: WebSocket = websocket
        # 연결 상태의 나머지: 구독 핸들 + 진행 중 버킷 + 마지막 전송 시각.
        # 생성 즉시 구독해, stream 태스크가 시작되기 전 도착한 이벤트도 큐에 쌓인다.
        self._subscription: Subscription = ticker_router.subscribe(ticker)
        self._bucket: _TickBucket = _TickBucket()
        self._last_sent_at: float | None = None
        self._state_changed: asyncio.Event = asyncio.Event()

    def close(self) -> None:
        """연결 종료 시 라우터 구독을 해제한다(동기, 중복 호출 무해)."""
        ticker_router.unsubscribe(self._subscription)

    def _apply_update(self, ticker: str, tick: int) -> None:
        """클라이언트의 갱신 요청을 연결 상태에 경쟁 조건 없이 반영한다.

        동기 메서드이므로(중간에 await 없음) stream/listen 태스크 사이에서 원자적
        으로 실행된다 — 라우팅/전송 도중 상태가 반쯤 바뀌는 인터리빙이 없다.

        ticker 가 바뀌면 새 구독을 먼저 만든 뒤 이전 구독을 해제하고 진행 중
        버킷을 비운다(이전 ticker 가격이 새 캔들에 섞이지 않도록). tick 만 바뀌면
        구독은 유지하고 집계 주기만 갱신한다(불필요한 재구독 방지).
        """
        ticker_changed = ticker != self.ticker
        tick_changed = tick != self.tick
        if not (ticker_changed or tick_changed):
            return

        if ticker_changed:
            old_ticker = self.ticker
            old = self._subscription
            self._subscription = ticker_router.subscribe(ticker)
            ticker_router.unsubscribe(old)
            self._bucket.reset()
            logger.info(f"ticker switched: {old_ticker} -> {ticker}")
        self.ticker = ticker
        self.tick = tick
        self._state_changed.set()

    async def listen_for_tick_updates(self) -> None:
        """클라이언트가 보내는 ticker/tick 갱신을 수신해 상태에 반영한다."""
        try:
            while True:
                try:
                    # 갱신이 없어도 주기적으로 깨어나 취소 신호에 응답한다.
                    raw = await asyncio.wait_for(
                        self.websocket.receive_json(),
                        timeout=_TICK_UPDATE_TIMEOUT_SECONDS,
                    )
                    # 초기 메시지와 동일하게 검증한다 — 잘못된 값이 상태·집계로
                    # 새는 것을 막는다(예: tick<=0, 빈 ticker).
                    update = RealtimeTickUpdate.model_validate(raw)
                    self._apply_update(update.ticker, update.tick)
                    logger.info(
                        f"connection state updated: "
                        f"ticker={self.ticker} tick={self.tick}"
                    )

                except asyncio.TimeoutError:
                    logger.debug("Waiting for user tick update")
                    continue

                except ValidationError as e:
                    # 잘못된 갱신 요청은 무시하고 기존 상태를 유지한다(연결 유지).
                    logger.warning(f"Ignoring invalid tick update: {e}")
                    continue

                except Exception as e:
                    logger.error(f"Error receiving tick update: {e}")
                    break
        except Exception as e:
            logger.error(f"Config listener error: {e}")

    async def stream_data(self) -> None:
        """구독 이벤트를 tick 윈도우로 묶어 캔들을 전송한다.

        각 윈도우 시작에서 현재 상태(구독/tick)를 스냅샷하고, 마감 시각까지 도착
        하는 이벤트의 가격/거래량으로 OHLCV 를 집계한 뒤 경계에서 flush 한다. 상태
        변경 이벤트를 함께 기다리므로 ticker 전환 시 기존 구독 대기를 즉시 중단하고
        새 구독 윈도우로 넘어간다.
        """
        loop = asyncio.get_running_loop()
        try:
            while True:
                # 윈도우 시작 상태 스냅샷(동기 읽기라 원자적).
                subscription = self._subscription
                ticker = self.ticker
                tick = self.tick
                self._state_changed.clear()

                self._bucket.reset()
                window_started_at = loop.time()
                deadline = window_started_at + tick
                restart_window = False

                # 마감 시각까지 남은 시간만큼만 대기하며 이벤트를 모은다.
                while (remaining := deadline - loop.time()) > 0:
                    event_task = asyncio.create_task(subscription.get())
                    state_task = asyncio.create_task(self._state_changed.wait())
                    done = set()
                    try:
                        done, _ = await asyncio.wait(
                            {event_task, state_task},
                            timeout=remaining,
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                    finally:
                        for task in (event_task, state_task):
                            if not task.done():
                                task.cancel()
                        await asyncio.gather(
                            event_task, state_task, return_exceptions=True
                        )

                    state_changed = state_task in done or self._state_changed.is_set()
                    if state_changed:
                        self._state_changed.clear()
                        if subscription is not self._subscription:
                            self._bucket.reset()
                            restart_window = True
                            break

                        ticker = self.ticker
                        tick = self.tick
                        deadline = window_started_at + tick
                        if event_task in done:
                            event = event_task.result()
                            self._bucket.add(float(event.price), event.volume)
                        continue

                    if event_task in done:
                        event = event_task.result()
                        self._bucket.add(float(event.price), event.volume)
                    else:
                        break

                if restart_window or subscription is not self._subscription:
                    continue
                await self._flush(ticker, tick)
        except Exception as e:
            logger.error(f"Data streaming error: {e}")

    async def _flush(self, ticker: str, tick: int) -> None:
        """현재 버킷을 캔들로 만들어 전송하고 마지막 전송 시각을 갱신한다."""
        bucket = self._bucket
        if (
            bucket.open_price is not None
            and bucket.high is not None
            and bucket.low is not None
            and bucket.close_price is not None
        ):
            data: TickData | None = TickData(
                open=bucket.open_price,
                high=bucket.high,
                low=bucket.low,
                close=bucket.close_price,
                volume=bucket.volume,
            )
        else:
            # 윈도우 내 데이터 없음 — 현재는 null 캔들(직전 종가 유지 등은 #6).
            data = None

        await self.websocket.send_json(
            TradeHighAndLow(
                type="candle_tick",
                ticker=ticker,
                data=data,
                current_tick=tick,
            )
        )
        self._bucket.reset()
        self._last_sent_at = asyncio.get_running_loop().time()
