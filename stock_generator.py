"""
초당 주식 거래 데이터를 생성하여 PostgreSQL 데이터베이스에 삽입
"""

import asyncio
import random
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Annotated, Literal

import numpy as np
from pydantic import BaseModel, Field, field_validator
from uuid_utils import UUID, uuid4, uuid7

from database.connector import get_connection
from utils.config_loader import get_config
from utils.logger import logger_instance

logger = logger_instance()


class TradeData(BaseModel):
    """개별 트레이드 데이터를 나타내는 Pydantic 모델 - 강화된 데이터 검증"""

    event_time: Annotated[datetime, Field(description="거래 발생 시간")]
    event_id: Annotated[UUID, Field(description="이벤트 고유 식별자")]
    ticker: Annotated[str, Field(min_length=1, max_length=10, description="주식 심볼")]
    price: Annotated[
        float, Field(gt=0, le=10000, description="거래 가격 (0 초과 10,000 이하)")
    ]
    volume: Annotated[
        int, Field(gt=0, le=1000000, description="거래량 (0 초과 1,000,000 이하)")
    ]
    trade_type: Annotated[Literal["BUY", "SELL"], Field(description="거래 유형")]
    trade_id: Annotated[UUID, Field(description="거래 고유 식별자")]
    market_code: Annotated[
        str, Field(min_length=1, max_length=20, description="시장 코드")
    ]
    currency_code: Annotated[
        str, Field(min_length=3, max_length=3, description="통화 코드 (3자리)")
    ]

    @field_validator("ticker")
    @classmethod
    def ticker_must_be_uppercase(cls, v):
        """ticker는 대문자여야 함"""
        return v.upper()

    @field_validator("currency_code")
    @classmethod
    def currency_code_must_be_uppercase(cls, v):
        """통화 코드는 대문자여야 함"""
        return v.upper()

    @field_validator("price")
    @classmethod
    def price_precision(cls, v):
        """가격은 소수점 둘째 자리까지만 허용"""
        return round(v, 2)

    def to_tuple(self) -> tuple:
        """데이터베이스 삽입을 위한 튜플 변환"""
        return (
            self.event_time,
            self.event_id,
            self.ticker,
            self.price,
            self.volume,
            self.trade_type,
            self.trade_id,
            self.market_code,
            self.currency_code,
        )

    model_config = {
        "arbitrary_types_allowed": True,
        "json_schema_extra": {
            "example": {
                "event_time": "2025-01-02T12:34:56.789Z",
                "event_id": "01234567-89ab-cdef-0123-456789abcdef",
                "ticker": "AAPL",
                "price": 150.25,
                "volume": 1000,
                "trade_type": "BUY",
                "trade_id": "fedcba98-7654-3210-fedc-ba9876543210",
                "market_code": "NASDAQ",
                "currency_code": "USD",
            }
        },
    }


@dataclass
class StockDataGenerator:
    """주식 데이터 생성기 - 설정 기반 초기화"""

    tickers: list[str] = field(default_factory=list)
    trade_types: list[str] = field(default_factory=list)
    market_code: str = ""
    currency_code: str = ""

    def __post_init__(self):
        """설정 파일에서 기본값들을 로드"""
        if not self.tickers:
            # 모든 섹터의 ticker들을 합쳐서 사용
            all_tickers = []
            ticker_sections = [
                "big_tech",
                "finance",
                "consumer",
                "healthcare",
                "energy",
                "retail",
                "industrial",
                "fintech",
                "media",
            ]

            for section in ticker_sections:
                section_tickers = get_config("stock_generator", "tickers", section)
                if section_tickers:
                    all_tickers.extend(section_tickers)

            self.tickers = all_tickers

        if not self.trade_types:
            self.trade_types = get_config("stock_generator", "market", "trade_types")

        if not self.market_code:
            self.market_code = get_config("stock_generator", "market", "market_code")

        if not self.currency_code:
            self.currency_code = get_config(
                "stock_generator", "market", "currency_code"
            )

    def generate_trade_batch(
        self, trade_transaction_per_second: int
    ) -> list[TradeData]:
        """트레이드 데이터 배치 생성 (현재시간 +1초부터 +10초까지) - NumPy 벡터화 최적화"""
        logger.info(
            f"📊 트레이드 데이터 배치 생성 중... (배치 크기: {trade_transaction_per_second}건)"
        )

        base_time: datetime = datetime.now(tz=UTC)
        base_timestamp = base_time.timestamp()

        # NumPy를 사용한 벡터화된 랜덤 값 생성
        rng = np.random.default_rng(42)

        # 설정에서 범위값들 로드
        time_min = get_config("stock_generator", "data_ranges", "time_offset_min")
        time_max = get_config("stock_generator", "data_ranges", "time_offset_max")
        price_min = get_config("stock_generator", "data_ranges", "price_min")
        price_max = get_config("stock_generator", "data_ranges", "price_max")
        volume_min = get_config("stock_generator", "data_ranges", "volume_min")
        volume_max = get_config("stock_generator", "data_ranges", "volume_max")

        # 시간 오프셋: 설정값 기반 랜덤한 값들
        time_offsets = rng.uniform(
            time_min, time_max, size=trade_transaction_per_second
        )

        # 가격: 설정값 기반 랜덤한 값들 (소수점 둘째 자리까지)
        prices = np.round(
            rng.uniform(price_min, price_max, size=trade_transaction_per_second), 2
        )

        # 거래량: 설정값 기반 랜덤한 정수들
        volumes = rng.integers(
            volume_min, volume_max + 1, size=trade_transaction_per_second
        )

        # ticker와 trade_type 인덱스를 랜덤하게 선택
        ticker_indices = rng.integers(
            0, len(self.tickers), size=trade_transaction_per_second
        )
        trade_type_indices = rng.integers(
            0, len(self.trade_types), size=trade_transaction_per_second
        )

        # TradeData 객체들 생성
        trades: list[TradeData] = []
        for i in range(trade_transaction_per_second):
            event_time = datetime.fromtimestamp(
                base_timestamp + time_offsets[i], tz=UTC
            )

            trade = TradeData(
                event_time=event_time,
                event_id=uuid7(),
                ticker=self.tickers[ticker_indices[i]],
                price=float(prices[i]),
                volume=int(volumes[i]),
                trade_type=self.trade_types[trade_type_indices[i]],
                trade_id=uuid4(),
                market_code=self.market_code,
                currency_code=self.currency_code,
            )
            trades.append(trade)

        logger.info(
            f"✅ 트레이드 데이터 배치 생성 완료 ({len(trades)}건, 시간범위: +1~+10초 밀리초 정밀도 랜덤분배) - NumPy 벡터화 적용"
        )
        return trades

    def generate_distributed_trades(
        self, trade_transaction_per_second: int
    ) -> list[TradeData]:
        """향후 10초간 각 1초마다 다른 랜덤 크기로 분산 생성 - NumPy 벡터화 최적화"""
        logger.info(
            f"📊 10초간 분산 트레이드 데이터 생성 시작 (총 목표: {trade_transaction_per_second}건)"
        )
        base_time: datetime = datetime.now(tz=UTC)
        base_timestamp = base_time.timestamp()

        # 총 batch_size를 10초에 걸쳐 랜덤하게 분배
        remaining_size = trade_transaction_per_second
        second_sizes = []

        # 처음 9초는 랜덤하게 분배
        for second in range(9):
            if remaining_size <= 1:
                second_sizes.append(0)
            else:
                # 남은 크기의 10~30% 정도를 랜덤하게 할당
                max_for_this_second = max(1, remaining_size // 3)
                size_for_this_second = random.randint(0, max_for_this_second)
                second_sizes.append(size_for_this_second)
                remaining_size -= size_for_this_second

        # 마지막 10초째에는 남은 모든 크기 할당
        second_sizes.append(remaining_size)

        logger.info(f"🎲 각 초별 데이터 크기: {second_sizes}")

        # NumPy를 사용한 벡터화된 랜덤 값 생성
        rng = np.random.default_rng(42)
        all_trades: list[TradeData] = []

        # 각 초별로 데이터 생성
        for second, size in enumerate(second_sizes):
            if size > 0:
                # 해당 초의 시간 범위 내에서 랜덤 시간들 생성 (벡터화)
                time_within_second = rng.uniform(0, 0.999, size=size)
                time_offsets = second + 1 + time_within_second

                # 설정에서 범위값들 로드
                price_min = get_config("stock_generator", "data_ranges", "price_min")
                price_max = get_config("stock_generator", "data_ranges", "price_max")
                volume_min = get_config("stock_generator", "data_ranges", "volume_min")
                volume_max = get_config("stock_generator", "data_ranges", "volume_max")

                # 가격, 거래량, ticker/trade_type 인덱스를 벡터화로 생성
                prices = np.round(rng.uniform(price_min, price_max, size=size), 2)
                volumes = rng.integers(volume_min, volume_max + 1, size=size)
                ticker_indices = rng.integers(0, len(self.tickers), size=size)
                trade_type_indices = rng.integers(0, len(self.trade_types), size=size)

                # TradeData 객체들 생성
                for i in range(size):
                    event_time = datetime.fromtimestamp(
                        base_timestamp + time_offsets[i], tz=UTC
                    )

                    trade = TradeData(
                        event_time=event_time,
                        event_id=uuid7(),
                        ticker=self.tickers[ticker_indices[i]],
                        price=float(prices[i]),
                        volume=int(volumes[i]),
                        trade_type=self.trade_types[trade_type_indices[i]],
                        trade_id=uuid4(),
                        market_code=self.market_code,
                        currency_code=self.currency_code,
                    )
                    all_trades.append(trade)

        logger.info(
            f"✅ 분산 트레이드 데이터 생성 완료 ({len(all_trades)}건) - NumPy 벡터화 적용"
        )
        return all_trades


@dataclass
class StockDataInserter:
    """주식 데이터를 데이터베이스에 삽입하는 클래스"""

    async def insert_trades_batch(self, trades: list[TradeData]) -> int:
        """트레이드 데이터 배치를 데이터베이스에 고성능 삽입 (COPY 명령 활용)"""
        if not trades:
            logger.warning("🔴 삽입할 트레이드 데이터가 없습니다")
            return 0

        logger.info(
            f"💾 데이터베이스에 {len(trades)}건의 트레이드 데이터 고성능 삽입 시작"
        )

        try:
            async with get_connection() as conn:
                # COPY를 위한 데이터 준비 - 튜플 리스트로 변환
                trade_tuples = [trade.to_tuple() for trade in trades]

                # copy_records_to_table을 사용하여 고성능 배치 삽입
                # PostgreSQL의 COPY 명령을 사용하므로 executemany보다 10-100배 빠름
                await conn.copy_records_to_table(
                    "stock_trades",
                    records=trade_tuples,
                    columns=[
                        "event_time",
                        "event_id",
                        "ticker",
                        "price",
                        "volume",
                        "trade_type",
                        "trade_id",
                        "market_code",
                        "currency_code",
                    ],
                )

                logger.info(
                    f"✅ {len(trades)}건의 트레이드 데이터 고성능 삽입 완료 (COPY 명령 사용)"
                )
                return len(trades)

        except Exception as e:
            logger.error(f"❌ 데이터베이스 고성능 삽입 중 오류 발생: {e}")
            # COPY 실패시 fallback으로 기존 방식 사용
            logger.info("🔄 COPY 실패, executemany 방식으로 재시도...")
            return await self._insert_trades_fallback(trades)

    async def _insert_trades_fallback(self, trades: list[TradeData]) -> int:
        """COPY 실패시 사용할 fallback 삽입 방식"""
        insert_query = """
            INSERT INTO stock_trades (
                event_time, event_id, ticker, price, volume, 
                trade_type, trade_id, market_code, currency_code
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """

        try:
            async with get_connection() as conn:
                trade_tuples = [trade.to_tuple() for trade in trades]
                await conn.executemany(insert_query, trade_tuples)
                logger.info(f"✅ {len(trades)}건의 트레이드 데이터 fallback 삽입 완료")
                return len(trades)
        except Exception as e:
            logger.error(f"❌ fallback 삽입 중 오류 발생: {e}")
            raise

    async def generate_and_insert(
        self, generator: StockDataGenerator, trade_transaction_per_second: int
    ) -> int:
        """데이터 생성과 동시에 데이터베이스에 삽입"""
        logger.info(
            f"🔄 {trade_transaction_per_second}건 데이터 생성 및 삽입 프로세스 시작"
        )

        # 데이터 생성
        trades = generator.generate_trade_batch(trade_transaction_per_second)

        # 데이터베이스에 삽입
        inserted_count = await self.insert_trades_batch(trades)

        logger.info(f"🎯 프로세스 완료: {inserted_count}건 데이터 생성 및 삽입")
        return inserted_count

    async def generate_distributed_and_insert(
        self, generator: StockDataGenerator, trade_transaction_per_second: int
    ) -> int:
        """10초간 분산된 데이터 생성과 동시에 데이터베이스에 삽입"""
        logger.info(
            f"🔄 {trade_transaction_per_second}건 분산 데이터 생성 및 삽입 프로세스 시작"
        )

        # 10초간 각 초별로 다른 크기로 분산 데이터 생성
        trades = generator.generate_distributed_trades(trade_transaction_per_second)

        # 데이터베이스에 삽입
        inserted_count = await self.insert_trades_batch(trades)

        logger.info(f"🎯 분산 프로세스 완료: {inserted_count}건 데이터 생성 및 삽입")
        return inserted_count


class StockDataGeneratorService:
    """애플리케이션 lifespan이 소유하는 주식 데이터 생성 태스크."""

    def __init__(self) -> None:
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()

    def start(self) -> bool:
        """생성 태스크를 한 번만 시작한다."""
        if self._task is not None and not self._task.done():
            return False

        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="stock-data-generator")
        return True

    async def stop(self) -> None:
        """실행 중인 생성 태스크를 취소하고 종료될 때까지 기다린다."""
        self._stop.set()
        if self._task is None:
            return

        if not self._task.done():
            self._task.cancel()
        await asyncio.gather(self._task, return_exceptions=True)
        self._task = None

    async def _run(self) -> None:
        """설정에 따라 주식 데이터를 생성하고 삽입한다."""
        trade_transaction_per_second = get_config(
            "stock_generator", "generation", "default_batch_size"
        )
        min_batch_size = get_config("stock_generator", "generation", "min_batch_size")
        max_batch_multiplier = get_config(
            "stock_generator", "generation", "max_batch_multiplier"
        )
        distribution_seconds = get_config(
            "stock_generator", "generation", "distribution_seconds"
        )

        generator = StockDataGenerator()
        inserter = StockDataInserter()

        logger.info(
            f"📈 주식 데이터 생성기 시작 - 기본 배치 크기: "
            f"{trade_transaction_per_second}, 최소: {min_batch_size}, "
            f"최대 배수: {max_batch_multiplier}x, "
            f"분산 시간: {distribution_seconds}초"
        )

        try:
            while not self._stop.is_set():
                try:
                    min_size = min_batch_size
                    max_size = trade_transaction_per_second * max_batch_multiplier
                    random_batch_size = random.randint(min_size, max_size)

                    logger.info(
                        f"🎲 랜덤 배치 크기 결정: {random_batch_size}개 "
                        f"(범위: {min_size}~{max_size})"
                    )

                    inserted_count = await inserter.generate_distributed_and_insert(
                        generator, trade_transaction_per_second=random_batch_size
                    )
                    logger.info(
                        f"📊 총 {inserted_count}건의 트레이드 데이터가 "
                        "데이터베이스에 삽입되었습니다"
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f"❌ 데이터 삽입 중 오류 발생: {e}")

                try:
                    await asyncio.wait_for(
                        self._stop.wait(), timeout=distribution_seconds
                    )
                except TimeoutError:
                    pass
        except asyncio.CancelledError:
            logger.info("Stock data generator cancelled")
            raise


stock_data_generator = StockDataGeneratorService()
