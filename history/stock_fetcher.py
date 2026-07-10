"""
PostgreSQL 의 지난 기간 주식 데이터를 조회
"""

import base64
import binascii
import json
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Literal, Self
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)
from pydantic.alias_generators import to_camel

from database import get_connection
from utils import logger_instance, serialize_value

from .validation_mixins import UppercaseAlphabetValidationMixin

logger = logger_instance()

MAX_QUERY_RANGE = timedelta(days=30)
MAX_QUERY_DURATION_MINUTES = int(MAX_QUERY_RANGE.total_seconds() // 60)
MAX_PAGE_LIMIT = 1000
Granularity = Literal["minute", "hour", "day"]
GRANULARITY_INTERVALS: dict[Granularity, str] = {
    "minute": "1 minute",
    "hour": "1 hour",
    "day": "1 day",
}


class StockTradeCursor(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: Literal[1]
    event_time: datetime
    tie_breaker: Annotated[str, Field(min_length=1)]
    granularity: Granularity | None

    @field_validator("event_time")
    @classmethod
    def normalize_event_time(cls, value: datetime) -> datetime:
        """Normalize cursor timestamps to UTC and reject ambiguous timestamps."""
        if value.tzinfo is None:
            raise ValueError("Cursor event_time must include a timezone")
        return value.astimezone(UTC)


def _encode_cursor(
    event_time: datetime,
    tie_breaker: str,
    granularity: Granularity | None,
) -> str:
    cursor = StockTradeCursor(
        version=1,
        event_time=event_time,
        tie_breaker=tie_breaker,
        granularity=granularity,
    )
    payload = json.dumps(
        cursor.model_dump(mode="json"),
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return base64.urlsafe_b64encode(payload).rstrip(b"=").decode("ascii")


def _decode_cursor(value: str) -> StockTradeCursor:
    try:
        encoded = value.encode("ascii")
        padding = b"=" * (-len(encoded) % 4)
        payload = base64.b64decode(
            encoded + padding,
            altchars=b"-_",
            validate=True,
        )
        return StockTradeCursor.model_validate_json(payload)
    except (
        UnicodeEncodeError,
        binascii.Error,
        ValidationError,
    ) as exc:
        raise ValueError("Invalid cursor") from exc


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
    granularity: Annotated[
        Granularity | None,
        Field(None, description="Aggregation granularity (minute/hour/day)"),
    ] = None
    cursor: Annotated[
        str | None,
        Field(None, description="Opaque cursor returned by the previous page"),
    ] = None
    limit: Annotated[
        int,
        Field(
            MAX_PAGE_LIMIT,
            description="Maximum number of trades returned per page",
            ge=1,
            le=MAX_PAGE_LIMIT,
        ),
    ] = MAX_PAGE_LIMIT

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

    @model_validator(mode="after")
    def validate_cursor(self) -> Self:
        """Validate the cursor structure and its query mode."""
        if self.cursor is None:
            return self

        cursor = _decode_cursor(self.cursor)
        if cursor.granularity != self.granularity:
            raise ValueError("Cursor granularity does not match the query")

        if self.granularity is None:
            try:
                UUID(cursor.tie_breaker)
            except ValueError as exc:
                raise ValueError("Invalid cursor") from exc

        return self


class StockTradeFilters(BaseModel):
    duration: int | None
    start_time: datetime | None
    end_time: datetime | None
    ticker: str | None
    trade_type: str | None
    market_code: str | None
    granularity: Granularity | None


class StockTradeResponse(BaseModel):
    data: list[dict[str, Any]]
    count: int
    filters: StockTradeFilters
    next_cursor: str | None
    has_more: bool


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
        cursor = _decode_cursor(query.cursor) if query.cursor is not None else None
        page_size = query.limit + 1

        if query.granularity is not None:
            interval = GRANULARITY_INTERVALS[query.granularity]
            bucket_expression = f"time_bucket('{interval}', event_time)"
            where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
            cursor_clause = ""
            if cursor is not None:
                cursor_param_index = len(params) + 1
                cursor_clause = (
                    "WHERE (event_time, ticker) "
                    f"< (${cursor_param_index}, ${cursor_param_index + 1})"
                )
                params.extend([cursor.event_time, cursor.tie_breaker])

            params.append(page_size)
            sql_query = f"""
                SELECT *
                FROM (
                    SELECT
                        {bucket_expression} AS event_time,
                        ticker,
                        first(price, event_time) AS open,
                        max(price) AS high,
                        min(price) AS low,
                        last(price, event_time) AS close,
                        sum(volume) AS volume,
                        count(*) AS trade_count
                    FROM stock_trades
                    {where_clause}
                    GROUP BY {bucket_expression}, ticker
                ) AS aggregated_trades
                {cursor_clause}
                ORDER BY event_time DESC, ticker DESC
                LIMIT ${len(params)}
            """
            tie_breaker_column = "ticker"
        else:
            if cursor is not None:
                cursor_param_index = len(params) + 1
                conditions.append(
                    "(event_time, event_id) "
                    f"< (${cursor_param_index}, ${cursor_param_index + 1})"
                )
                params.extend([cursor.event_time, UUID(cursor.tie_breaker)])

            where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
            params.append(page_size)
            sql_query = (
                "SELECT * FROM stock_trades"
                + where_clause
                + " ORDER BY event_time DESC, event_id DESC"
                + f" LIMIT ${len(params)}"
            )
            tie_breaker_column = "event_id"

        async with get_connection() as conn:
            result = await conn.fetch(sql_query, *params)
            has_more = len(result) > query.limit
            page_result = result[: query.limit]
            next_cursor = None
            if has_more and page_result:
                last_record = page_result[-1]
                next_cursor = _encode_cursor(
                    event_time=last_record["event_time"],
                    tie_breaker=str(last_record[tie_breaker_column]),
                    granularity=query.granularity,
                )

            logger.info(f"Fetched {len(page_result)} stock trades with filters")

            serialized_result: list[dict[str, Any]] = [
                {key: serialize_value(value) for key, value in dict(record).items()}
                for record in page_result
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
                    granularity=query.granularity,
                ),
                next_cursor=next_cursor,
                has_more=has_more,
            )
