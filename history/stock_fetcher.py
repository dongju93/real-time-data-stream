"""
PostgreSQL 의 지난 기간 주식 데이터를 조회
"""

from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic.alias_generators import to_camel

from database import get_connection
from utils import logger_instance, serialize_value

from .validation_mixins import UppercaseAlphabetValidationMixin

logger = logger_instance()

MAX_QUERY_RANGE = timedelta(days=30)
MAX_QUERY_DURATION_MINUTES = int(MAX_QUERY_RANGE.total_seconds() // 60)


class StockTradeQuery(BaseModel, UppercaseAlphabetValidationMixin):
    model_config = ConfigDict(alias_generator=to_camel, extra="forbid")

    duration: Annotated[
        int | None,
        Field(
            None,
            description="Duration in minutes from current time",
            ge=1,
            le=MAX_QUERY_DURATION_MINUTES,
        ),
    ] = None
    start_time: Annotated[
        datetime | None,
        Field(None, description="Range start time in ISO 8601 format"),
    ] = None
    end_time: Annotated[
        datetime | None,
        Field(None, description="Range end time in ISO 8601 format"),
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

    @field_validator("start_time", "end_time")
    @classmethod
    def normalize_time(cls, value: datetime | None) -> datetime | None:
        """Normalize range timestamps to UTC, treating naive values as UTC."""
        if value is None:
            return value
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_time_filters(self) -> Self:
        """Validate duration and explicit time range filters."""
        has_start_time = self.start_time is not None
        has_end_time = self.end_time is not None

        if has_start_time != has_end_time:
            raise ValueError("startTime and endTime must be provided together")

        if self.duration is not None and has_start_time:
            raise ValueError("duration cannot be used with startTime and endTime")

        if self.start_time is None or self.end_time is None:
            return self

        if self.start_time >= self.end_time:
            raise ValueError("startTime must be earlier than endTime")

        if self.end_time - self.start_time > MAX_QUERY_RANGE:
            raise ValueError("Time range cannot exceed 30 days")

        now = datetime.now(tz=UTC)
        if self.start_time > now or self.end_time > now:
            raise ValueError("startTime and endTime cannot be in the future")

        return self


class StockTradeFilters(BaseModel):
    duration: int | None
    start_time: datetime | None
    end_time: datetime | None
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
        conditions: list[str] = []
        params: list[Any] = []
        # Index for parameterized queries position
        param_index = 1

        # Add event_time range filter if explicit range is provided
        if query.start_time is not None and query.end_time is not None:
            conditions.append(
                f"event_time BETWEEN ${param_index} AND ${param_index + 1}"
            )
            params.extend([query.start_time, query.end_time])
            param_index += 2

        # Otherwise, preserve the duration-based event_time filter
        elif query.duration is not None:
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
                    start_time=query.start_time,
                    end_time=query.end_time,
                    ticker=query.ticker,
                    trade_type=query.trade_type,
                    market_code=query.market_code,
                ),
            )
