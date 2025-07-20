"""
PostgreSQL 의 지난 기간 주식 데이터를 조회
"""

from datetime import datetime, timedelta
from typing import Any

from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel

from database import get_connection
from utils import logger_instance, serialize_value

logger = logger_instance()


class StockTradeQuery(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, extra="forbid")

    duration: int | None = Field(
        None, description="Duration in minutes from current time", ge=1
    )
    ticker: str | None = Field(None, description="Stock ticker symbol")
    trade_type: str | None = Field(None, description="Trade type (BUY/SELL)")
    market_code: str | None = Field(None, description="Market code")


class StockTradeFilters(BaseModel):
    duration: int | None
    ticker: str | None
    trade_type: str | None
    market_code: str | None


class StockTradeResponse(BaseModel):
    data: list[dict[str, Any]]
    count: int
    filters: StockTradeFilters


class StockTradeRepository:
    """Repository class for handling stock trade data operations with SQL injection protection."""

    @classmethod
    def _validate_ticker(cls, ticker: str) -> str:
        """Validate ticker symbol format."""
        if not all(c.isalnum() or c in [".", "-", "_"] for c in ticker):
            raise ValueError("Invalid ticker symbol format")
        return ticker

    @classmethod
    def _validate_trade_type(cls, trade_type: str) -> str:
        """Validate trade type."""
        trade_type_upper = trade_type.upper()
        if trade_type_upper not in ["BUY", "SELL"]:
            raise ValueError("Invalid trade type - must be BUY or SELL")
        return trade_type_upper

    @classmethod
    def _validate_market_code(cls, market_code: str) -> str:
        """Validate market code format."""
        if not market_code.isalnum():
            raise ValueError("Invalid market code format")
        return market_code

    @classmethod
    def _build_query_conditions(
        cls, query: StockTradeQuery
    ) -> tuple[list[str], list[Any]]:
        """Build parameterized query conditions with validation."""
        conditions = []
        params = []
        param_count = 1

        # Add event_time filter if duration is provided
        if query.duration is not None:
            start_time = datetime.now() - timedelta(minutes=query.duration)
            conditions.append(f"event_time >= ${param_count}")
            params.append(start_time)
            param_count += 1

        # Add ticker filter with validation
        if query.ticker is not None:
            validated_ticker = cls._validate_ticker(query.ticker)
            conditions.append(f"ticker = ${param_count}")
            params.append(validated_ticker)
            param_count += 1

        # Add trade_type filter with validation
        if query.trade_type is not None:
            validated_trade_type = cls._validate_trade_type(query.trade_type)
            conditions.append(f"trade_type = ${param_count}")
            params.append(validated_trade_type)
            param_count += 1

        # Add market_code filter with validation
        if query.market_code is not None:
            validated_market_code = cls._validate_market_code(query.market_code)
            conditions.append(f"market_code = ${param_count}")
            params.append(validated_market_code)
            param_count += 1

        return conditions, params

    @classmethod
    async def fetch_trades(cls, query: StockTradeQuery) -> StockTradeResponse:
        """Fetch stock trades from the database with optional filters and SQL injection protection.

        Args:
            query: StockTradeQuery containing filter parameters

        Returns:
            StockTradeResponse: Filtered stock trades with metadata
        """
        conditions, params = cls._build_query_conditions(query)

        # Build the complete query
        base_query = "SELECT * FROM stock_trades"
        if conditions:
            where_clause = " WHERE " + " AND ".join(conditions)
            sql_query = (
                base_query + where_clause + " ORDER BY event_time DESC LIMIT 1000"
            )
        else:
            sql_query = base_query + " ORDER BY event_time DESC LIMIT 1000"

        async with get_connection() as conn:
            result = await conn.fetch(sql_query, *params)
            logger.info(f"Fetched {len(result)} stock trades with filters")

            serialized_result: list[dict[str, Any]] = [
                {key: serialize_value(value) for key, value in dict(record).items()}
                for record in result
            ]

            return StockTradeResponse(
                data=serialized_result,
                count=len(serialized_result),
                filters=StockTradeFilters(
                    duration=query.duration,
                    ticker=query.ticker,
                    trade_type=query.trade_type,
                    market_code=query.market_code,
                ),
            )
