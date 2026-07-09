"""fan-out consumer group 은 프로세스마다 유일해야 한다.

동일 group_id 를 multi-worker 가 공유하면 Kafka 가 파티션을 로드밸런싱해
로컬 ticker_router 구독자가 일부 체결을 놓친다. 이 테스트는 suffix 규칙과
settings 로드 경로를 고정한다.
"""

import os
import re

from messaging.config import fanout_instance_group_id, load_kafka_settings


def test_fanout_instance_group_id_includes_base_host_and_pid() -> None:
    base = "real-time-data-stream-realtime"
    group_id = fanout_instance_group_id(base)

    assert group_id.startswith(f"{base}-")
    assert group_id.endswith(f"-{os.getpid()}")
    # base 와 hostname-pid 사이 separator 가 유지된다
    assert group_id != base
    # Kafka group.id 에 위험한 공백/슬래시 등이 들어가지 않는다
    assert re.fullmatch(r"[A-Za-z0-9._-]+", group_id)


def test_load_kafka_settings_uses_instance_unique_group_id() -> None:
    settings = load_kafka_settings()
    expected = fanout_instance_group_id("real-time-data-stream-realtime")

    assert settings.group_id == expected
    assert settings.to_consumer_config()["group.id"] == expected
