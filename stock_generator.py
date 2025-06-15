"""
초당 주식 거래 데이터를 생성하여 PostgreSQL 데이터베이스에 삽입
"""

import asyncio
import random
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import NoReturn

from uuid_utils import UUID, uuid4, uuid7

from database.connector import get_connection
from utils.logger import logger_instance

logger = logger_instance()


@dataclass
class TradeData:
    """개별 트레이드 데이터를 나타내는 데이터클래스"""

    event_time: datetime
    event_id: UUID
    ticker: str
    price: float
    volume: int
    trade_type: str
    trade_id: UUID
    market_code: str
    currency_code: str

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


@dataclass
class StockDataGenerator:
    """주식 데이터 생성기"""

    tickers: list[str]
    trade_types: list[str] = field(default_factory=lambda: ["BUY", "SELL"])  # immutable
    market_code: str = "NASDAQ"
    currency_code: str = "USD"

    def generate_trade_batch(
        self, trade_transaction_per_second: int
    ) -> list[TradeData]:
        """트레이드 데이터 배치 생성 (현재시간 +1초부터 +10초까지)"""
        logger.info(
            f"📊 트레이드 데이터 배치 생성 중... (배치 크기: {trade_transaction_per_second}건)"
        )
        trades: list[TradeData] = []
        base_time: datetime = datetime.now(tz=UTC)

        for _ in range(trade_transaction_per_second):
            # 현재시간 +1초부터 +10초까지 밀리초 정밀도로 랜덤 분배
            time_offset: float = random.uniform(1.0, 10.0)
            event_time: datetime = datetime.fromtimestamp(
                base_time.timestamp() + time_offset, tz=UTC
            )

            trade = TradeData(
                event_time=event_time,
                event_id=uuid7(),
                ticker=random.choice(self.tickers),
                price=round(random.uniform(100.0, 500.0), 2),
                volume=random.randint(1, 1000),
                trade_type=random.choice(self.trade_types),
                trade_id=uuid4(),
                market_code=self.market_code,
                currency_code=self.currency_code,
            )
            trades.append(trade)

        logger.info(
            f"✅ 트레이드 데이터 배치 생성 완료 ({len(trades)}건, 시간범위: +1~+10초 밀리초 정밀도 랜덤분배)"
        )
        return trades

    def generate_distributed_trades(
        self, trade_transaction_per_second: int
    ) -> list[TradeData]:
        """향후 10초간 각 1초마다 다른 랜덤 크기로 분산 생성"""
        logger.info(
            f"📊 10초간 분산 트레이드 데이터 생성 시작 (총 목표: {trade_transaction_per_second}건)"
        )
        all_trades: list[TradeData] = []
        base_time: datetime = datetime.now(tz=UTC)

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

        # 각 초별로 데이터 생성
        for second, size in enumerate(second_sizes):
            if size > 0:
                for _ in range(size):
                    # 해당 초의 시간 범위 내에서 랜덤 시간 설정
                    time_offset = second + 1 + random.uniform(0, 0.999)
                    event_time = datetime.fromtimestamp(
                        base_time.timestamp() + time_offset, tz=UTC
                    )

                    trade = TradeData(
                        event_time=event_time,
                        event_id=uuid7(),
                        ticker=random.choice(self.tickers),
                        price=round(random.uniform(100.0, 500.0), 2),
                        volume=random.randint(1, 1000),
                        trade_type=random.choice(self.trade_types),
                        trade_id=uuid4(),
                        market_code=self.market_code,
                        currency_code=self.currency_code,
                    )
                    all_trades.append(trade)

        logger.info(f"✅ 분산 트레이드 데이터 생성 완료 ({len(all_trades)}건)")
        return all_trades


@dataclass
class StockDataInserter:
    """주식 데이터를 데이터베이스에 삽입하는 클래스"""

    async def insert_trades_batch(self, trades: list[TradeData]) -> int:
        """트레이드 데이터 배치를 데이터베이스에 삽입"""
        if not trades:
            logger.warning("🔴 삽입할 트레이드 데이터가 없습니다")
            return 0

        logger.info(f"💾 데이터베이스에 {len(trades)}건의 트레이드 데이터 삽입 시작")

        # 삽입 쿼리 정의 (stock_trades 테이블 스키마에 맞춤)
        insert_query = """
            INSERT INTO stock_trades (
                event_time, event_id, ticker, price, volume, 
                trade_type, trade_id, market_code, currency_code
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """

        try:
            async with get_connection() as conn:
                # 배치 삽입을 위한 데이터 준비
                trade_tuples = [trade.to_tuple() for trade in trades]

                # executemany를 사용하여 배치 삽입
                await conn.executemany(insert_query, trade_tuples)

                logger.info(f"✅ {len(trades)}건의 트레이드 데이터 삽입 완료")
                return len(trades)

            # 연결을 얻지 못한 경우
            logger.error("❌ 데이터베이스 연결을 얻을 수 없습니다")
            return 0

        except Exception as e:
            logger.error(f"❌ 데이터베이스 삽입 중 오류 발생: {e}")
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


async def run_stock_data_inserter(trade_transaction_per_second: int = 1000) -> NoReturn:
    """주식 데이터 생성 및 삽입 실행 - 10초마다 10~(trade_transaction_per_second*10)개의 랜덤 크기 데이터 생성"""
    tickers = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]
    generator = StockDataGenerator(tickers)
    inserter = StockDataInserter()

    while True:
        try:
            # 10개부터 (trade_transaction_per_second*10)개까지 랜덤한 크기로 데이터 생성
            min_size = 10
            max_size = trade_transaction_per_second * 10
            random_batch_size = random.randint(min_size, max_size)

            logger.info(
                f"🎲 랜덤 배치 크기 결정: {random_batch_size}개 (범위: {min_size}~{max_size})"
            )

            # 10초간 각 초별로 다른 크기로 분산 트레이드 데이터 생성 및 삽입
            inserted_count = await inserter.generate_distributed_and_insert(
                generator, trade_transaction_per_second=random_batch_size
            )
            logger.info(
                f"📊 총 {inserted_count}건의 트레이드 데이터가 데이터베이스에 삽입되었습니다"
            )
        except Exception as e:
            logger.error(f"❌ 데이터 삽입 중 오류 발생: {e}")

        # 10초 대기 후 다음 배치 생성
        await asyncio.sleep(10)
