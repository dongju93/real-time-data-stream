"""
PostgreSQL 의 지난 기간 주식 데이터를 조회
"""

from datetime import UTC, datetime, timedelta
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic.alias_generators import to_camel

from database import get_connection
from utils import logger_instance, serialize_value

from .validation_mixins import UppercaseAlphabetValidationMixin

logger = logger_instance()


class StockTradeQuery(BaseModel, UppercaseAlphabetValidationMixin):
    model_config = ConfigDict(alias_generator=to_camel, extra="forbid")

    duration: Annotated[
        int | None,
        Field(None, description="Duration in minutes from current time", ge=1),
    ] = None
    ticker: Annotated[str | None, Field(None, description="Stock ticker symbol")] = None
    trade_type: Annotated[
        str | None, Field(None, description="Trade type (BUY/SELL)")
    ] = None
    market_code: Annotated[str | None, Field(None, description="Market code")] = None

    @field_validator("trade_type")
    @classmethod
    def validate_trade_type(cls, value: str | None) -> str | None:
        """Validate trade type."""
        if value is None:
            return value
        value_upper: str = value.upper()
        if value_upper not in ["BUY", "SELL"]:
            raise ValueError("Invalid trade type - must be BUY or SELL")
        return value_upper


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
    def _build_query_conditions(
        cls, query: StockTradeQuery
    ) -> tuple[list[str], list[Any]]:
        """Build parameterized query conditions with validation."""
        conditions = []
        params = []
        # Index for parameterized queries position
        param_index = 1

        # Add event_time filter if duration is provided
        if query.duration is not None:
            start_time: datetime = datetime.now(tz=UTC) - timedelta(
                minutes=query.duration
            )
            conditions.append(f"event_time >= ${param_index}")
            params.append(start_time)
            param_index += 1

        # Add ticker filter
        if query.ticker is not None:
            conditions.append(f"ticker = ${param_index}")
            params.append(query.ticker)
            param_index += 1

        # Add trade_type filter
        if query.trade_type is not None:
            conditions.append(f"trade_type = ${param_index}")
            params.append(query.trade_type)
            param_index += 1

        # Add market_code filter
        if query.market_code is not None:
            conditions.append(f"market_code = ${param_index}")
            params.append(query.market_code)
            param_index += 1

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
            where_clause: str = " WHERE " + " AND ".join(conditions)
            sql_query: str = (
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
