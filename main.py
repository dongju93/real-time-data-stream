from fastapi import APIRouter, BackgroundTasks, FastAPI, status
from fastapi.responses import ORJSONResponse

from database.connector import get_connection
from stock_generator import run_stock_data_inserter
from utils.logger import logger_instance
from utils.serializer import serialize_value

logger = logger_instance()

stock_streamer = FastAPI(default_response_class=ORJSONResponse)

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


stock_streamer.include_router(stock_streamer_v1)
