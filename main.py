import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import APIRouter, BackgroundTasks, FastAPI, status
from fastapi.responses import ORJSONResponse, StreamingResponse
from fastapi.websockets import WebSocket, WebSocketDisconnect

from anomaly import AnomalyStreamer
from database import get_connection
from database.connector import db_pool
from realtime import TickStreamer, TickUpdate
from stock_generator import run_stock_data_inserter
from utils import logger_instance, serialize_value

logger = logger_instance()


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:
    # 애플리케이션 시작 시 데이터베이스 풀 생성
    await db_pool.create()
    logger.info("Database connection pool created successfully")

    yield

    # 애플리케이션 종료 시 데이터베이스 풀 해제
    await db_pool.close()
    logger.info("Database connection pool closed successfully")


stock_streamer = FastAPI(default_response_class=ORJSONResponse, lifespan=lifespan)

stock_streamer_v1 = APIRouter(prefix="/api/v1")


@stock_streamer_v1.get("/test")
async def fetch_stock_data() -> ORJSONResponse:
    """Fetch stock data from the database.

    Returns:
        dict: A dictionary containing the fetched stock data.
    """
    async with get_connection() as conn:
        result = await conn.fetch("SELECT * FROM stock_trades LIMIT 10")
        logger.info("Fetched stock data successfully")

        serialized_result: list[dict[str, str | int]] = [
            {key: serialize_value(value) for key, value in dict(record).items()}
            for record in result
        ]

        return ORJSONResponse(content=serialized_result, status_code=status.HTTP_200_OK)


@stock_streamer_v1.post("/stock/generate")
async def generate_stock_data(background_tasks: BackgroundTasks) -> ORJSONResponse:
    """Generate stock data in the background.

    Args:
        background_tasks (BackgroundTasks): Background tasks manager.

    Returns:
        ORJSONResponse: Response indicating the status of the operation.
    """
    background_tasks.add_task(run_stock_data_inserter)
    return ORJSONResponse(
        content={"status": "success", "message": "Stock data generation started"},
        status_code=status.HTTP_202_ACCEPTED,
    )


@stock_streamer_v1.websocket("/stock/realtime")
async def stream_realtime_stock_data(websocket: WebSocket) -> None:
    await websocket.accept()
    logger.info("WebSocket connection established")

    initial_tick: TickUpdate = await websocket.receive_json()
    ticker: str = initial_tick["ticker"]
    tick: int = initial_tick["tick"]

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
