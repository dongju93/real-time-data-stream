"""Kafka 설정 로딩.

`env.toml` 의 `[kafka.*]` 섹션을 `utils.config_loader` 로 읽어 검증된
Pydantic 모델로 반환한다. 하드코딩된 접속 정보를 두지 않기 위한 중앙화
지점이며(개발 항목 #17), consumer 팩토리가 이 모델만 받도록 한다.
"""

from typing import Any, Literal

from pydantic import BaseModel, Field

from utils.config_loader import get_config

# librdkafka 의 auto.offset.reset 이 허용하는 값. 오타를 기동 시점에 잡기 위해
# 자유 문자열이 아닌 Literal 로 제한한다.
type AutoOffsetReset = Literal["earliest", "latest", "none"]


class KafkaSettings(BaseModel):
    """Kafka consumer 를 구성하는 검증된 설정값.

    `env.toml` 로부터 로드되며, `to_consumer_config()` 로 confluent-kafka 가
    기대하는 점(.) 표기 설정 dict 로 변환된다.
    """

    bootstrap_servers: str = Field(min_length=1, description="브로커 접속 주소")
    group_id: str = Field(min_length=1, description="consumer group id")
    auto_offset_reset: AutoOffsetReset = "latest"
    enable_auto_commit: bool = True
    stock_trades_topic: str = Field(min_length=1, description="Debezium 체결 CDC 토픽")

    def to_consumer_config(self) -> dict[str, Any]:
        """confluent-kafka `Consumer` 가 받는 설정 dict 로 변환.

        Pydantic 필드명(snake_case)을 librdkafka 설정 키(dot.case)로 매핑한다.
        """
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": self.enable_auto_commit,
        }


def load_kafka_settings() -> KafkaSettings:
    """`env.toml` 에서 Kafka 설정을 읽어 검증된 `KafkaSettings` 로 반환."""
    return KafkaSettings(
        bootstrap_servers=get_config("kafka", "connection", "bootstrap_servers"),
        group_id=get_config("kafka", "consumer", "group_id"),
        auto_offset_reset=get_config("kafka", "consumer", "auto_offset_reset"),
        enable_auto_commit=get_config("kafka", "consumer", "enable_auto_commit"),
        stock_trades_topic=get_config("kafka", "topics", "stock_trades"),
    )
