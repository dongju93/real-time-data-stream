import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Annotated, Any

from fastapi import (
    APIRouter,
    Depends,
    FastAPI,
    Request,
    status,
)
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.websockets import WebSocket, WebSocketDisconnect
from pydantic import BaseModel, ConfigDict, Field
from starlette.exceptions import HTTPException as StarletteHTTPException

from anomaly import AnomalyStreamer
from database import get_connection
from database.connector import db_pool
from history import StockTradeQuery, StockTradeRepository, StockTradeResponse
from messaging import stock_trade_consumer
from realtime import TickStreamer, ticker_router
from realtime.model import RealtimeTickUpdate
from stock_generator import (
    StockGenerationRequest,
    StockGenerationStartResponse,
    StockGenerationStatus,
    stock_data_generator,
)
from utils import logger_instance, serialize_value

logger = logger_instance()


class APIErrorDetail(BaseModel):
    """A field-level request validation error."""

    model_config = ConfigDict(extra="forbid")

    field: str
    message: str
    type: str


class APIErrorResponse(BaseModel):
    """Standard HTTP error response."""

    model_config = ConfigDict(extra="forbid")

    code: str
    message: str
    details: list[APIErrorDetail] = Field(default_factory=list)


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncGenerator[None, None]:
    # 애플리케이션 시작 시 데이터베이스 풀 생성
    await db_pool.create()
    logger.info("Database connection pool created successfully")

    # 조립 지점(#3): 공유 consumer 가 파싱한 CDC 이벤트 배치를 연결별 fan-out
    # 라우터로 흘려보내도록 연결한다. consumer 는 라우터를 직접 임포트하지 않고
    # 이 콜백 seam 으로만 결합해 순환 임포트를 피한다.
    stock_trade_consumer.set_event_handler(ticker_router.route)

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


@stock_streamer.exception_handler(RequestValidationError)
async def handle_request_validation_error(
    _: Request,
    exc: RequestValidationError,
) -> JSONResponse:
    details = [
        APIErrorDetail(
            field=".".join(str(part) for part in error["loc"]),
            message=error["msg"],
            type=error["type"],
        )
        for error in exc.errors()
    ]
    response = APIErrorResponse(
        code="validation_error",
        message="Request validation failed",
        details=details,
    )
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        content=response.model_dump(mode="json"),
    )


@stock_streamer.exception_handler(StarletteHTTPException)
async def handle_http_exception(
    _: Request,
    exc: StarletteHTTPException,
) -> JSONResponse:
    response = APIErrorResponse(
        code="http_error",
        message=exc.detail if isinstance(exc.detail, str) else "Request failed",
    )
    return JSONResponse(
        status_code=exc.status_code,
        content=response.model_dump(mode="json"),
        headers=exc.headers,
    )


@stock_streamer.exception_handler(Exception)
async def handle_unexpected_exception(request: Request, exc: Exception) -> JSONResponse:
    logger.exception(
        "Unhandled error while processing %s %s",
        request.method,
        request.url.path,
        exc_info=exc,
    )
    response = APIErrorResponse(
        code="internal_server_error",
        message="Failed to process request",
    )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=response.model_dump(mode="json"),
    )


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


@stock_streamer_v1.post(
    "/stock/generate",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=StockGenerationStartResponse,
)
async def generate_stock_data(
    request: StockGenerationRequest | None = None,
) -> StockGenerationStartResponse:
    """Start continuous generation or a finite historical performance seed."""
    if request is not None and request.mode == "historical":
        started = stock_data_generator.start_historical(request)
    else:
        started = stock_data_generator.start()

    return StockGenerationStartResponse(
        message="started" if started else "already running",
        generation=stock_data_generator.status(),
    )


@stock_streamer_v1.get(
    "/stock/generate/status",
    response_model=StockGenerationStatus,
)
async def get_stock_generation_status() -> StockGenerationStatus:
    """Return progress for continuous generation or a historical seed."""
    return stock_data_generator.status()


@stock_streamer_v1.get(
    "/stock",
    responses={
        status.HTTP_422_UNPROCESSABLE_CONTENT: {
            "model": APIErrorResponse,
            "description": "Invalid query parameters",
        },
        status.HTTP_500_INTERNAL_SERVER_ERROR: {
            "model": APIErrorResponse,
            "description": "Internal server error",
        },
    },
)
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
    return await StockTradeRepository.fetch_trades(query)


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

    # 연결 상태 소유자를 try 밖에서 만들어(생성 시 라우터 구독) finally 가 항상
    # 구독을 해제할 수 있게 한다 — 연결 누수/유령 구독 방지.
    tick_streamer = TickStreamer(
        ticker,
        tick,
        websocket,
        consumer_health_check=stock_trade_consumer.raise_if_fatal,
        consumer_stale_check=stock_trade_consumer.is_stale,
    )

    try:
        logger.info(f"Starting real-time stream for {ticker} with {tick}s tick")

        # Create both tasks
        tick_listen_task = asyncio.create_task(tick_streamer.listen_for_tick_updates())
        tick_stream_task = asyncio.create_task(tick_streamer.stream_data())

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

        # 라우터 구독 해제(#3): 태스크 정리 후 마지막에 수행해 유령 구독을 남기지
        # 않는다. 태스크 취소를 먼저 끝내야 stream 태스크가 이미 해제된 구독을
        # 다시 건드리지 않는다.
        tick_streamer.close()


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
