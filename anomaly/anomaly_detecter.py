"""
Apache Flink 를 통해 검출된 거래 이상 데이터를 처리
"""

import asyncio
from datetime import UTC, datetime
from typing import Any, AsyncGenerator

import orjson


class AnomalyStreamer:
    """SSE 스트림 관리 클래스"""

    def __init__(self, interval: float = 5.0) -> None:
        self.interval: float = interval
        self._is_active: bool = True
        self.event_type: str = "anomaly"

    async def generate_sse_stream(self) -> AsyncGenerator[str, None]:
        """SSE 스트림 생성기"""
        try:
            while self._is_active:
                data: dict[str, str] = {
                    "timestamp": datetime.now(UTC).isoformat(),
                    "anomaly_data": "anomaly data goes here",
                }

                sse_data: str = self._format_sse_data(data)

                yield sse_data

                # 지정된 간격으로 대기
                await asyncio.sleep(self.interval)

        except asyncio.CancelledError:
            # 연결 종료 시 정리 작업
            self._is_active = False
            yield self._format_sse_data(
                {"event": "connection_closed", "message": "스트림이 종료되었습니다."}
            )

    def _format_sse_data(self, data: dict[str, Any]) -> str:
        """데이터를 SSE 형식으로 포맷팅"""

        sse_message: str = f"event: {self.event_type}\n"  # SSE event type
        sse_message += f"data: {orjson.dumps(data).decode('utf-8')}\n"
        sse_message += "\n"  # End of message

        return sse_message

    def stop_stream(self) -> None:
        """스트림 중지"""
        self._is_active = False
