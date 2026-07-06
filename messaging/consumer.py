"""Kafka 체결 데이터 consumer 의 수명주기 관리.

`confluent-kafka` 2.15 의 네이티브 async 클라이언트(`confluent_kafka.aio.AIOConsumer`)
를 사용한다. 내부적으로 librdkafka(C)의 blocking 호출을 ThreadPoolExecutor 로
감싸 코루틴으로 노출하므로, 이벤트 루프를 막지 않으면서 C 기반의 높은 처리량을
얻는다(개발 항목 #1 의 "confluent-kafka + executor" 방식을 라이브러리가 대신 수행).

`DatabasePool.create()/close()` 패턴을 그대로 따르되, DB 풀과 달리 백그라운드
소비 루프(`asyncio.Task`)를 함께 소유한다. 이 모듈은 브로커 연결·수신을 로그로
확인하는 데까지만 책임진다(#1 완료 기준). 메시지 파싱(#2)·ticker 라우팅(#3)은
후속 항목에서 이 소비 루프 위에 얹는다.
"""

import asyncio

from confluent_kafka import KafkaError, Message
from confluent_kafka.aio import AIOConsumer

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
        self._message_count: int = 0

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
            f"Kafka consumer closed (total messages consumed: {self._message_count})"
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
                    self._log_batch(trades)
        except asyncio.CancelledError:
            logger.info("Kafka consume loop cancelled")
            raise
        except Exception as e:
            logger.error(f"Kafka consume loop error: {e}")

    def _is_trade_message(self, message: Message) -> bool:
        """유효한 체결 메시지면 True, 에러/파티션 끝이면 False.

        #1 범위에서는 데이터 payload 를 파싱하지 않고 수신 여부만 판별한다.
        Debezium envelope 파싱→도메인 이벤트 변환은 #2 에서 얹는다.
        """
        error: KafkaError | None = message.error()
        if error is None:
            return True
        # 파티션 끝 도달은 정상 상황(에러 아님)이라 조용히 넘긴다.
        if error.code() != KafkaError._PARTITION_EOF:
            logger.warning(f"Kafka message error: {error.str()}")
        return False

    def _log_batch(self, trades: list[Message]) -> None:
        """배치 단위 요약 로깅.

        초당 수천 건에서도 로그가 폭주하지 않도록 배치당 한 줄만 남기고,
        마지막 메시지의 오프셋과 payload 앞부분만 샘플로 보여준다(#1 수신 확인).
        """
        sample_message = trades[-1]
        raw_value: bytes | None = sample_message.value()
        sample = (
            raw_value[:120].decode("utf-8", errors="replace") if raw_value else None
        )
        logger.info(
            f"Consumed {len(trades)} stock trades (total={self._message_count}) "
            f"[partition={sample_message.partition()} offset={sample_message.offset()}] "
            f"sample={sample}"
        )


# 앱 전역에서 공유하는 단일 consumer 인스턴스(db_pool 과 동일한 싱글턴 패턴).
stock_trade_consumer = StockTradeConsumer()
