import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Annotated, Any

from fastapi import (
    APIRouter,
    Depends,
    FastAPI,
    HTTPException,
    status,
)
from fastapi.responses import StreamingResponse
from fastapi.websockets import WebSocket, WebSocketDisconnect

from anomaly import AnomalyStreamer
from database import get_connection
from database.connector import db_pool
from history import StockTradeQuery, StockTradeRepository, StockTradeResponse
from messaging import stock_trade_consumer
from realtime import TickStreamer
from realtime.model import RealtimeTickUpdate
from stock_generator import stock_data_generator
from utils import logger_instance, serialize_value

logger = logger_instance()


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncGenerator[None, None]:
    # 애플리케이션 시작 시 데이터베이스 풀 생성
    await db_pool.create()
    logger.info("Database connection pool created successfully")

    # Kafka 체결 데이터 consumer 시작 (브로커 구독 + 백그라운드 소비 루프)
    await stock_trade_consumer.create()
    logger.info("Kafka stock-trade consumer started successfully")

    yield

    # 장수명 태스크를 먼저 중지한 뒤 해당 태스크가 사용하는 리소스를 정리한다
    await stock_data_generator.stop()
    logger.info("Stock data generator stopped successfully")

    await stock_trade_consumer.close()
    logger.info("Kafka stock-trade consumer stopped successfully")

    # 애플리케이션 종료 시 데이터베이스 풀 해제
    await db_pool.close()
    logger.info("Database connection pool closed successfully")


stock_streamer = FastAPI(lifespan=lifespan)

stock_streamer_v1 = APIRouter(prefix="/api/v1")


@stock_streamer_v1.get("/test")
async def fetch_stock_data() -> list[dict[str, Any]]:
    """Fetch stock data from the database.

    Returns:
        list[dict[str, Any]]: The fetched stock records. FastAPI serializes this
        directly to JSON via Pydantic (datetime/UUID/Decimal handled by the
        encoder), so no explicit response class is needed.
    """
    async with get_connection() as conn:
        result = await conn.fetch("SELECT * FROM stock_trades LIMIT 10")
        logger.info("Fetched stock data successfully")

        return [
            {key: serialize_value(value) for key, value in dict(record).items()}
            for record in result
        ]


@stock_streamer_v1.post("/stock/generate", status_code=status.HTTP_202_ACCEPTED)
async def generate_stock_data() -> dict[str, str]:
    """Start the lifespan-managed stock data generator."""
    started = stock_data_generator.start()
    return {
        "status": "success",
        "message": "started" if started else "already running",
    }


@stock_streamer_v1.get("/stock")
async def get_stock_trades(
    query: Annotated[StockTradeQuery, Depends()],
) -> StockTradeResponse:
    """Fetch stock trades from the database with optional filters.

    Args:
        query: StockTradeQuery containing filter parameters

    Returns:
        StockTradeResponse: Filtered stock trades. Returning the model directly
        lets FastAPI serialize it (and document it in the OpenAPI schema).
    """
    try:
        return await StockTradeRepository.fetch_trades(query)

    except Exception as e:
        logger.error(f"Error fetching stock trades: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch stock trades",
        ) from e


# wss
@stock_streamer_v1.websocket("/stock/real-time")
async def stream_realtime_stock_data(websocket: WebSocket) -> None:
    await websocket.accept()
    logger.info("WebSocket connection established")

    # 1. get data from websocket message
    raw_data = await websocket.receive_json()
    # 2. validate date from get data
    initial_tick: RealtimeTickUpdate = RealtimeTickUpdate.model_validate(raw_data)
    # 3. allocate each vars
    ticker: str = initial_tick.ticker
    tick: int = initial_tick.tick

    tick_listen_task: asyncio.Task[None] | None = None
    tick_stream_task: asyncio.Task[None] | None = None

    try:
        logger.info(f"Starting real-time stream for {ticker} with {tick}s tick")

        stock_streamer = TickStreamer(ticker, tick, websocket)

        # Create both tasks
        tick_listen_task = asyncio.create_task(stock_streamer.listen_for_tick_updates())
        tick_stream_task = asyncio.create_task(stock_streamer.stream_data())

        # Run both task indefinitely
        await asyncio.gather(tick_listen_task, tick_stream_task)

    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for ticker: {ticker}")
    except Exception as e:
        logger.error(f"Error in WebSocket stream: {e}")
        await websocket.close(1011)
    finally:
        tasks_to_cancel = []
        if tick_listen_task:
            tick_listen_task.cancel()  # Signal the task to cancel
            tasks_to_cancel.append(tick_listen_task)
        if tick_stream_task:
            tick_stream_task.cancel()
            tasks_to_cancel.append(tick_stream_task)

        # Wait tasks are cancelled then clean up
        if tasks_to_cancel:
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
            logger.info("Tasks cancelled and cleaned up")


@stock_streamer_v1.get("/stock/anomaly")
async def stream_anomaly_stock_transaction() -> StreamingResponse:
    anomaly_streamer = AnomalyStreamer(5.0)

    return StreamingResponse(
        anomaly_streamer.generate_sse_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


stock_streamer.include_router(stock_streamer_v1)
