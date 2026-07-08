"""Kafka 체결 데이터 consumer 의 수명주기 관리.

`confluent-kafka` 2.15 의 네이티브 async 클라이언트(`confluent_kafka.aio.AIOConsumer`)
를 사용한다. 내부적으로 librdkafka(C)의 blocking 호출을 ThreadPoolExecutor 로
감싸 코루틴으로 노출하므로, 이벤트 루프를 막지 않으면서 C 기반의 높은 처리량을
얻는다(개발 항목 #1 의 "confluent-kafka + executor" 방식을 라이브러리가 대신 수행).

`DatabasePool.create()/close()` 패턴을 그대로 따르되, DB 풀과 달리 백그라운드
소비 루프(`asyncio.Task`)를 함께 소유한다. 소비 루프는 수신 확인(#1) 위에
Debezium envelope → 도메인 이벤트(`CdcTradeEvent`) 파싱(#2)을 얹고, 파싱된
이벤트 배치를 등록된 핸들러(#3 fan-out 라우터)로 넘긴다. 배치 단위 요약만 로깅한다.

파싱 로직은 도메인 모델과 함께 `realtime.model` 에 있다(#2 명세). 의존은
messaging → realtime.model 단방향으로 유지한다. #3 의 fan-out 라우터는 여기서
직접 임포트하지 않고 `set_event_handler()` 콜백 seam 으로 주입받아, main.py
(조립 지점)가 `stock_trade_consumer.set_event_handler(ticker_router.route)` 로
연결하게 한다 — 역방향 결합을 조립 지점에 몰아 순환 임포트를 피한다.
"""

import asyncio
from collections.abc import Callable

from confluent_kafka import KafkaError, Message
from confluent_kafka.aio import AIOConsumer

from realtime.model import CdcMessageParseError, CdcTradeEvent, parse_cdc_message
from utils.logger import logger_instance

from .config import KafkaSettings, load_kafka_settings

logger = logger_instance()

# 한 번의 consume() 호출로 최대 몇 건을 받을지. executor 왕복 비용을 배치에
# 분산해 처리량을 높인다. timeout 은 데이터가 없어도 루프가 주기적으로 깨어나
# 취소 신호에 응답하도록 하는 상한(초).
_CONSUME_BATCH_SIZE = 500
_CONSUME_TIMEOUT_SECONDS = 1.0


class StockTradeConsumer:
    """`stock.public.stock_trades` 토픽을 소비하는 공유 consumer.

    전 ticker 를 하나의 consumer 로 읽어들이는 구조로, 향후 연결별 fan-out
    라우팅(#3)의 기반이 된다. 연결마다 consumer 를 만드는 대신 공유 1개를 두어
    1,000 동시연결(성능요구 #1)에서도 브로커 부담을 낮춘다.
    """

    def __init__(self, settings: KafkaSettings | None = None) -> None:
        # 설정은 create() 시점에 확정한다(테스트에서 주입 가능하도록 인자 허용).
        self._settings: KafkaSettings | None = settings
        self._consumer: AIOConsumer | None = None
        self._consume_task: asyncio.Task[None] | None = None
        # 파싱된 CDC 이벤트 배치를 받을 핸들러(#3 fan-out 라우터 연결점).
        # main.py 가 set_event_handler() 로 주입한다. 미설정이면 이벤트는 버려진다.
        self._on_events: Callable[[list[CdcTradeEvent]], None] | None = None
        # 운영 카운터(#18 의 기초): 수신·파싱 성공·정책상 무시·파싱 실패 누계.
        self._message_count: int = 0
        self._parsed_count: int = 0
        self._skipped_count: int = 0
        self._parse_failure_count: int = 0

    def set_event_handler(
        self, handler: Callable[[list[CdcTradeEvent]], None] | None
    ) -> None:
        """파싱된 CDC 이벤트 배치를 받을 콜백을 등록한다(#3 fan-out 라우터 연결점).

        핸들러는 소비 루프(이벤트 루프 스레드)에서 **동기·논블로킹**으로 호출되므로
        여기서 블로킹하면 전체 소비가 멈춘다. `ticker_router.route` 가 이 계약을
        만족한다. `create()` 전후 언제든 호출 가능하다.
        """
        self._on_events = handler

    async def create(self) -> None:
        """AIOConsumer 를 생성·구독하고 백그라운드 소비 루프를 시작한다.

        반드시 실행 중인 이벤트 루프(=FastAPI lifespan) 안에서 호출해야 한다.
        AIOConsumer 생성자가 현재 이벤트 루프에 콜백을 바인딩하기 때문이다.
        """
        if self._consumer is not None:
            return

        settings = self._settings or load_kafka_settings()
        self._settings = settings

        # 브로커가 잠시 죽어 있어도 여기서 예외가 나지 않는다. librdkafka 는
        # 백그라운드에서 지연 연결하므로, 앱은 기동되고 연결 실패는 소비 루프의
        # 에러 로그로 드러난다(운영 복원력).
        self._consumer = AIOConsumer(settings.to_consumer_config())
        await self._consumer.subscribe([settings.stock_trades_topic])

        self._consume_task = asyncio.create_task(
            self._consume_loop(), name="kafka-stock-trades-consume-loop"
        )
        logger.info(
            f"Kafka consumer subscribed to '{settings.stock_trades_topic}' "
            f"(bootstrap={settings.bootstrap_servers}, group={settings.group_id})"
        )

    async def close(self) -> None:
        """소비 루프를 취소하고 consumer 를 정리한다."""
        if self._consume_task is not None:
            self._consume_task.cancel()
            # 취소가 실제로 완료될 때까지 대기(예외는 삼킨다).
            await asyncio.gather(self._consume_task, return_exceptions=True)
            self._consume_task = None

        if self._consumer is not None:
            # 오프셋 커밋 flush 및 group leave 를 포함한 정상 종료.
            await self._consumer.close()
            self._consumer = None

        logger.info(
            "Kafka consumer closed "
            f"(consumed={self._message_count}, parsed={self._parsed_count}, "
            f"skipped={self._skipped_count}, "
            f"parse_failures={self._parse_failure_count})"
        )

    async def _consume_loop(self) -> None:
        """브로커에서 메시지를 배치로 받아 처리한다.

        CancelledError 는 정상 종료 신호이므로 재전파한다. 그 외 예외는 루프를
        죽이지 않도록 로깅 후 짧게 대기하고 계속한다(#6 오류 복구의 기초).
        """
        assert self._consumer is not None

        try:
            while True:
                messages: list[Message] = await self._consumer.consume(
                    num_messages=_CONSUME_BATCH_SIZE,
                    timeout=_CONSUME_TIMEOUT_SECONDS,
                )
                trades = [msg for msg in messages if self._is_trade_message(msg)]
                if trades:
                    self._message_count += len(trades)
                    events = self._process_batch(trades)
                    # fan-out 라우팅(#3): 파싱된 이벤트를 ticker 별 구독자에게
                    # 분배한다. 핸들러 오류가 소비 루프를 죽이지 않도록 격리한다.
                    if events and self._on_events is not None:
                        try:
                            self._on_events(events)
                        except Exception as e:
                            logger.error(
                                f"Event fan-out handler failed "
                                f"for {len(events)} events: {e}"
                            )
        except asyncio.CancelledError:
            logger.info("Kafka consume loop cancelled")
            raise
        except Exception as e:
            logger.error(f"Kafka consume loop error: {e}")

    def _is_trade_message(self, message: Message) -> bool:
        """유효한 체결 메시지면 True, 에러/파티션 끝이면 False.

        여기서는 Kafka 수준의 수신 유효성만 판별한다. payload 해석은
        `_process_batch` 가 호출하는 `parse_cdc_message`(#2)의 몫이다.
        """
        error: KafkaError | None = message.error()
        if error is None:
            return True
        # 파티션 끝 도달은 정상 상황(에러 아님)이라 조용히 넘긴다.
        if error.code() != KafkaError._PARTITION_EOF:
            logger.warning(f"Kafka message error: {error.str()}")
        return False

    def _process_batch(self, trades: list[Message]) -> list[CdcTradeEvent]:
        """배치를 Debezium envelope → 도메인 이벤트로 변환하고 요약 로깅한다(#2).

        파싱 실패는 배치를 멈추지 않는다 — 실패는 건수로 세고 마지막 오류만
        경고 한 줄로 남긴다(초당 수천 건 환경의 배치당 1줄 로깅 규약).
        반환값(검증된 이벤트 리스트)이 #3 ticker 라우팅의 입력이 된다.
        """
        events: list[CdcTradeEvent] = []
        skipped = 0
        failed = 0
        last_error: CdcMessageParseError | None = None

        for message in trades:
            raw_value: bytes | None = message.value()
            try:
                event = parse_cdc_message(raw_value)
            except CdcMessageParseError as e:
                failed += 1
                last_error = e
                continue
            if event is None:
                # tombstone 이거나 u/d 등 정책상 무시하는 op(#2 정책).
                skipped += 1
            else:
                events.append(event)

        self._parsed_count += len(events)
        self._skipped_count += skipped
        self._parse_failure_count += failed

        last_message = trades[-1]
        sample = (
            f"{events[-1].ticker} {events[-1].trade_type} "
            f"{events[-1].volume}@{events[-1].price}"
            if events
            else None
        )
        logger.info(
            f"Consumed {len(trades)} CDC messages "
            f"(parsed={len(events)}, skipped={skipped}, failed={failed}, "
            f"total_parsed={self._parsed_count}) "
            f"[partition={last_message.partition()} offset={last_message.offset()}] "
            f"sample={sample}"
        )
        if last_error is not None:
            logger.warning(
                f"CDC parse failures in batch: {failed} (last error: {last_error})"
            )
        return events


# 앱 전역에서 공유하는 단일 consumer 인스턴스(db_pool 과 동일한 싱글턴 패턴).
stock_trade_consumer = StockTradeConsumer()
