"""Kafka 설정 로딩.

`env.toml` 의 `[kafka.*]` 섹션을 `utils.config_loader` 로 읽어 검증된
Pydantic 모델로 반환한다. 하드코딩된 접속 정보를 두지 않기 위한 중앙화
지점이며(개발 항목 #17), consumer 팩토리가 이 모델만 받도록 한다.
"""

import os
import re
import socket
from typing import Any, Literal

from pydantic import BaseModel, Field

from utils.config_loader import get_config

# librdkafka 의 auto.offset.reset 이 허용하는 값. 오타를 기동 시점에 잡기 위해
# 자유 문자열이 아닌 Literal 로 제한한다.
type AutoOffsetReset = Literal["earliest", "latest", "none"]

# Kafka group.id 에 안전한 문자만 남긴다(호스트명 등 외부 입력 정제).
_GROUP_ID_SAFE = re.compile(r"[^A-Za-z0-9._-]+")


class KafkaSettings(BaseModel):
    """Kafka consumer 를 구성하는 검증된 설정값.

    `env.toml` 로부터 로드되며, `to_consumer_config()` 로 confluent-kafka 가
    기대하는 점(.) 표기 설정 dict 로 변환된다.

    `group_id` 는 fan-out 인스턴스마다 유일해야 한다. 여러 worker/replica 가
    같은 group 을 쓰면 파티션이 로드밸런싱되어 로컬 `ticker_router` 가 일부
    체결을 영원히 못 받는다 — `load_kafka_settings()` 가 프로세스 단위 suffix
    를 붙여 이를 방지한다.
    """

    bootstrap_servers: str = Field(min_length=1, description="브로커 접속 주소")
    group_id: str = Field(
        min_length=1, description="consumer group id (instance-unique)"
    )
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


def fanout_instance_group_id(base_group_id: str) -> str:
    """fan-out 용 consumer group id 를 프로세스마다 유일하게 만든다.

    공유 group 은 Kafka 가 파티션을 멤버 간에 분할 할당한다. 이 앱은 프로세스
    내 `ticker_router` 로 WebSocket 구독자에게 분배하므로, **각 프로세스가
    토픽 전체를 받아야** 한다. 인스턴스별 group 이면 멤버가 1명이라 모든
    파티션이 그 프로세스에 할당된다(독립 소비 = 브로드캐스트).

    Debezium 행 키가 파티션을 가르면 한 ticker 이벤트도 여러 파티션에 흩어질
    수 있어, 공유 group + multi-worker 조합은 특히 위험하다.

    suffix 는 `{hostname}-{pid}`: 같은 호스트 multi-worker 와 컨테이너 복제본
    (호스트명=파드명) 모두 구분된다. 실시간 시세는 `auto.offset.reset=latest`
    이므로 재기동 시 오프셋 연속성은 필요 없다.
    """
    host = _GROUP_ID_SAFE.sub("-", socket.gethostname()).strip("-") or "host"
    return f"{base_group_id}-{host}-{os.getpid()}"


def load_kafka_settings() -> KafkaSettings:
    """`env.toml` 에서 Kafka 설정을 읽어 검증된 `KafkaSettings` 로 반환.

    `group_id` 는 env 의 base 값에 프로세스 식별자를 붙여 인스턴스 유일로 만든다.
    테스트에서 고정 group 이 필요하면 `KafkaSettings(...)` 를 직접 주입한다.
    """
    base_group_id = get_config("kafka", "consumer", "group_id")
    return KafkaSettings(
        bootstrap_servers=get_config("kafka", "connection", "bootstrap_servers"),
        group_id=fanout_instance_group_id(base_group_id),
        auto_offset_reset=get_config("kafka", "consumer", "auto_offset_reset"),
        enable_auto_commit=get_config("kafka", "consumer", "enable_auto_commit"),
        stock_trades_topic=get_config("kafka", "topics", "stock_trades"),
    )
