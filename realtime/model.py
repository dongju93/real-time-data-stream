from typing import Annotated, Literal, TypedDict

from pydantic import BaseModel, Field


class TickUpdate(TypedDict):
    ticker: str
    tick: int


class TickData(TypedDict):
    high: float
    low: float


class TradeHighAndLow(TypedDict):
    type: Literal["candle_tick"]
    ticker: str
    data: TickData | None
    current_tick: int


class RealtimeTickUpdate(BaseModel):
    ticker: Annotated[
        str,
        Field(
            min_length=1,
            max_length=10,
            title="Stock Ticker",
            description="The stock ticker symbol",
            example="AAPL",
        ),
    ]
    tick: Annotated[
        int,  # s 단위, 향후 ms 단위로 개선해야함
        Field(
            gt=0,
            le=60,  # 실시간 시세창에서 1분 이상 rate 는 의미 없음
            title="Tick Interval",
            description="The interval in seconds for data updates",
            example=5,
        ),
    ]
