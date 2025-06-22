import asyncio
from typing import TypedDict

from fastapi import WebSocket

from database.connector import get_connection
from utils.logger import logger_instance

logger = logger_instance()


class TickUpdate(TypedDict):
    ticker: str
    tick: int


class TickData(TypedDict):
    high: float
    low: float


class StockStreamer:
    def __init__(
        self,
        ticker: str,
        tick: int,
        websocket: WebSocket,
    ) -> None:
        self.ticker: str = ticker
        self.tick: int = tick
        self.websocket: WebSocket = websocket

    async def listen_for_tick_updates(self) -> None:
        """Listen for tick updates from client"""
        try:
            while True:
                try:
                    # wait non-blocking for tick updates but 60s timeout for active listening
                    tick_update: TickUpdate = await asyncio.wait_for(
                        self.websocket.receive_json(), timeout=60.0
                    )
                    self.ticker = tick_update["ticker"]
                    self.tick = tick_update["tick"]
                    logger.info(f"tick updated to: {self.tick}")

                except asyncio.TimeoutError:
                    logger.debug("Waiting for user tick update")
                    continue

                except Exception as e:
                    logger.error(f"Error receiving tick update: {e}")
                    break
        except Exception as e:
            logger.error(f"Config listener error: {e}")

    async def stream_data(
        self,
    ) -> None:
        """Stream stock data at the current tick"""
        try:
            while True:
                # 🚧 static data query for test
                async with get_connection() as conn:
                    result = await conn.fetch(
                        "SELECT price FROM stock_trades WHERE ticker = $1 ORDER BY event_time DESC LIMIT 1",
                        self.ticker,
                    )

                    if result:
                        # Candle data serialization
                        stock_data: dict[str, str | int] = dict(result[0])
                        serialized_data: TickData = TickData(
                            high=float(stock_data["price"]),
                            low=float(stock_data["price"]),
                        )

                        await self.websocket.send_json(
                            {
                                "type": "candle_tick",
                                "ticker": self.ticker,
                                "data": serialized_data,
                                "current_tick": self.tick,
                            }
                        )
                    else:
                        # // TODO: If no data found, just wait tick data
                        await self.websocket.send_json(
                            {
                                "type": "candle_tick",
                                "ticker": self.ticker,
                                "message": None,
                                "current_tick": self.tick,
                            }
                        )

                await asyncio.sleep(self.tick)
        except Exception as e:
            logger.error(f"Data streaming error: {e}")
