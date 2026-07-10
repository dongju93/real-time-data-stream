"""
PostgreSQL 의 지난 기간 주식 데이터를 조회
"""

import base64
import binascii
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Annotated, Any, Literal, Self
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)
from pydantic.alias_generators import to_camel

from database import get_connection
from utils import logger_instance

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
    start_time: datetime | None = None
    end_time: datetime | None = None

    @field_validator("event_time")
    @classmethod
    def normalize_event_time(cls, value: datetime) -> datetime:
        """Normalize cursor timestamps to UTC and reject ambiguous timestamps."""
        if value.tzinfo is None:
            raise ValueError("Cursor event_time must include a timezone")
        return value.astimezone(UTC)

    @field_validator("start_time", "end_time")
    @classmethod
    def normalize_range_time(cls, value: datetime | None) -> datetime | None:
        """Normalize optional cursor range timestamps to UTC."""
        if value is None:
            return value
        if value.tzinfo is None:
            raise ValueError("Cursor time range must include a timezone")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_time_range(self) -> Self:
        """Validate the optional fixed time range carried by the cursor."""
        if (self.start_time is None) != (self.end_time is None):
            raise ValueError("Cursor time range is incomplete")
        if (
            self.start_time is not None
            and self.end_time is not None
            and self.start_time >= self.end_time
        ):
            raise ValueError("Cursor time range is invalid")
        return self


def _encode_cursor(
    event_time: datetime,
    tie_breaker: str,
    granularity: Granularity | None,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
) -> str:
    cursor = StockTradeCursor(
        version=1,
        event_time=event_time,
        tie_breaker=tie_breaker,
        granularity=granularity,
        start_time=start_time,
        end_time=end_time,
    )
    payload = json.dumps(
        cursor.model_dump(
            mode="json",
            exclude={"start_time", "end_time"} if cursor.start_time is None else None,
        ),
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

        if self.duration is not None:
            if cursor.start_time is None or cursor.end_time is None:
                raise ValueError("Duration cursor does not contain a fixed time range")
            if cursor.end_time - cursor.start_time != timedelta(minutes=self.duration):
                raise ValueError("Cursor duration does not match the query")
        elif cursor.start_time is not None or cursor.end_time is not None:
            raise ValueError("Duration cursor requires duration")

        return self


class StockTrade(BaseModel):
    """A single persisted stock trade."""

    model_config = ConfigDict(extra="forbid")

    event_time: AwareDatetime
    event_id: UUID
    ticker: Annotated[str, Field(min_length=1, max_length=10)]
    price: Annotated[Decimal, Field(max_digits=12, decimal_places=2)]
    volume: int
    trade_type: Annotated[str, Field(min_length=1, max_length=10)]
    trade_id: UUID
    market_code: Annotated[str | None, Field(max_length=10)] = None
    currency_code: Annotated[
        str | None,
        Field(min_length=3, max_length=3),
    ] = None


class StockTradeAggregate(BaseModel):
    """An OHLCV bucket returned for an aggregated stock trade query."""

    model_config = ConfigDict(extra="forbid")

    event_time: AwareDatetime
    ticker: Annotated[str, Field(min_length=1, max_length=10)]
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    trade_count: Annotated[int, Field(ge=0)]


class StockTradeFilters(BaseModel):
    """Filter values applied to the query."""

    model_config = ConfigDict(extra="forbid")

    duration: int | None
    ticker: str | None
    trade_type: str | None
    market_code: str | None


class StockTradeTimeRange(BaseModel):
    """Resolved UTC time range applied to the query."""

    model_config = ConfigDict(extra="forbid")

    start_time: AwareDatetime
    end_time: AwareDatetime


class StockTradeCursorMetadata(BaseModel):
    """Cursor pagination state for the current response."""

    model_config = ConfigDict(extra="forbid")

    current: str | None
    next: str | None
    limit: Annotated[int, Field(ge=1, le=MAX_PAGE_LIMIT)]
    has_more: bool


class StockTradeResponseMetadata(BaseModel):
    """Standard metadata shared by raw and aggregated trade responses."""

    model_config = ConfigDict(extra="forbid")

    count: Annotated[int, Field(ge=0)]
    filters: StockTradeFilters
    granularity: Granularity | None
    time_range: StockTradeTimeRange | None
    cursor: StockTradeCursorMetadata


class StockTradeResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    data: list[StockTrade] | list[StockTradeAggregate]
    metadata: StockTradeResponseMetadata


class StockTradeRepository:
    """Repository class for handling stock trade data operations with SQL injection protection."""

    @classmethod
    def _build_query_conditions(
        cls,
        query: StockTradeQuery,
        time_range: StockTradeTimeRange | None,
    ) -> tuple[list[str], list[Any]]:
        """Build parameterized query conditions with validation."""
        conditions: list[str] = []
        params: list[Any] = []
        # Index for parameterized queries position
        param_index = 1

        # Apply the exact resolved range also exposed in response metadata.
        if time_range is not None:
            conditions.append(
                f"event_time BETWEEN ${param_index} AND ${param_index + 1}"
            )
            params.extend([time_range.start_time, time_range.end_time])
            param_index += 2

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

    @staticmethod
    def _resolve_time_range(
        query: StockTradeQuery,
        cursor: StockTradeCursor | None = None,
    ) -> StockTradeTimeRange | None:
        """Resolve explicit or duration-based filters to one UTC time range."""
        if query.start_time is not None and query.end_time is not None:
            return StockTradeTimeRange(
                start_time=query.start_time,
                end_time=query.end_time,
            )

        if query.duration is None:
            return None

        if cursor is not None:
            if cursor.start_time is None or cursor.end_time is None:
                raise ValueError("Duration cursor does not contain a fixed time range")
            return StockTradeTimeRange(
                start_time=cursor.start_time,
                end_time=cursor.end_time,
            )

        end_time = datetime.now(tz=UTC)
        return StockTradeTimeRange(
            start_time=end_time - timedelta(minutes=query.duration),
            end_time=end_time,
        )

    @classmethod
    async def fetch_trades(cls, query: StockTradeQuery) -> StockTradeResponse:
        """Fetch stock trades from the database with optional filters and SQL injection protection.

        Args:
            query: StockTradeQuery containing filter parameters

        Returns:
            StockTradeResponse: Filtered stock trades with metadata
        """
        cursor = _decode_cursor(query.cursor) if query.cursor is not None else None
        time_range = cls._resolve_time_range(query, cursor)
        conditions, params = cls._build_query_conditions(query, time_range)
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
                    start_time=(
                        time_range.start_time
                        if query.duration is not None and time_range is not None
                        else None
                    ),
                    end_time=(
                        time_range.end_time
                        if query.duration is not None and time_range is not None
                        else None
                    ),
                )

            logger.info(f"Fetched {len(page_result)} stock trades with filters")

            data: list[StockTrade] | list[StockTradeAggregate]
            if query.granularity is None:
                data = [
                    StockTrade.model_validate(dict(record)) for record in page_result
                ]
            else:
                data = [
                    StockTradeAggregate.model_validate(dict(record))
                    for record in page_result
                ]

            return StockTradeResponse(
                data=data,
                metadata=StockTradeResponseMetadata(
                    count=len(data),
                    filters=StockTradeFilters(
                        duration=query.duration,
                        ticker=query.ticker,
                        trade_type=query.trade_type,
                        market_code=query.market_code,
                    ),
                    granularity=query.granularity,
                    time_range=time_range,
                    cursor=StockTradeCursorMetadata(
                        current=query.cursor,
                        next=next_cursor,
                        limit=query.limit,
                        has_more=has_more,
                    ),
                ),
            )
