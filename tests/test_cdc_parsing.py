"""개발 항목 #2 완료 기준 검증: raw Kafka 메시지 1건 → 검증된 도메인 이벤트.

Debezium(PostgreSQL pgoutput, JsonConverter, value.converter.schemas.enable=false)
이 `stock.public.stock_trades` 토픽으로 발행하는 실제 wire format 을 재현해
`parse_cdc_message` 의 변환·무시·실패 정책을 단위 테스트한다.

실행: 저장소 루트에서 `uv run pytest`
(env.toml 이 cwd 상대 경로로 로드되므로 루트 실행이 전제다 — pyproject 의
`[tool.pytest.ini_options]` 참고)
"""

import base64
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import orjson
import pytest

from realtime.model import CdcMessageParseError, CdcTradeEvent, parse_cdc_message

_UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def encode_debezium_decimal(value: str, scale: int = 2) -> str:
    """`decimal.handling.mode=precise` 의 base64(unscaled bytes) 인코딩 재현.

    Java BigDecimal.unscaledValue().toByteArray() 와 동일하게 최소 길이의
    big-endian two's-complement 바이트로 만든 뒤 base64 문자열로 감싼다.
    (예: "150.25" → unscaled 15025 → 0x3AB1 → "OrE=")
    """
    unscaled = int(Decimal(value).scaleb(scale))
    n_bytes = max(1, (unscaled.bit_length() + 8) // 8)
    return base64.b64encode(unscaled.to_bytes(n_bytes, "big", signed=True)).decode()


def make_after(**overrides: Any) -> dict[str, Any]:
    """stock_trades 1행의 `after` 이미지 — 전 컬럼을 wire 표현으로 담는다."""
    after: dict[str, Any] = {
        # TIMESTAMPTZ → io.debezium.time.ZonedTimestamp (ISO-8601 문자열)
        "event_time": "2026-07-07T04:05:06.123456Z",
        "event_id": "0197d1f0-5f3a-7000-8000-000000000000",
        "ticker": "AAPL",
        # DECIMAL(12,2) → 기본값 precise + BASE64: unscaled bytes 의 base64
        "price": encode_debezium_decimal("150.25"),
        "volume": 1000,
        "trade_type": "BUY",
        "trade_id": "9b2e6a1c-1111-4222-8333-444455556666",
        "market_code": "NASDAQ",
        "currency_code": "USD",
    }
    after.update(overrides)
    return after


def make_message(
    op: str = "c",
    after: dict[str, Any] | None = None,
    **envelope_overrides: Any,
) -> bytes:
    """schemas.enable=false 기준의 Debezium change event 를 bytes 로 직렬화."""
    envelope: dict[str, Any] = {
        "before": None,
        "after": after,
        "source": {
            "version": "3.0.0.Final",
            "connector": "postgresql",
            "name": "stock",
            "ts_ms": 1751861106123,
            "db": "stock",
            # TimescaleDb SMT 가 chunk 이벤트를 재라우팅하므로 source 는
            # 하이퍼테이블이 아니라 내부 chunk 테이블을 가리킨다.
            "schema": "_timescaledb_internal",
            "table": "_hyper_1_1_chunk",
        },
        "op": op,
        "ts_ms": 1751861106456,
        "transaction": None,
    }
    envelope.update(envelope_overrides)
    return orjson.dumps(envelope)


class TestInsertParsing:
    """op=c(+snapshot r) 메시지가 검증된 도메인 이벤트로 변환된다."""

    def test_insert_message_becomes_validated_domain_event(self) -> None:
        event = parse_cdc_message(make_message(op="c", after=make_after()))

        assert isinstance(event, CdcTradeEvent)
        assert event.ticker == "AAPL"
        assert event.price == Decimal("150.25")
        assert event.event_time == datetime(2026, 7, 7, 4, 5, 6, 123456, tzinfo=UTC)
        assert event.volume == 1000
        assert event.trade_type == "BUY"
        assert event.market_code == "NASDAQ"

    def test_snapshot_read_op_is_parsed(self) -> None:
        event = parse_cdc_message(make_message(op="r", after=make_after()))
        assert event is not None
        assert event.ticker == "AAPL"

    def test_str_payload_is_accepted(self) -> None:
        raw = make_message(after=make_after()).decode()
        event = parse_cdc_message(raw)
        assert event is not None

    def test_null_market_code_is_allowed(self) -> None:
        event = parse_cdc_message(make_message(after=make_after(market_code=None)))
        assert event is not None
        assert event.market_code is None

    def test_schema_wrapped_payload_is_unwrapped(self) -> None:
        """워커의 schemas.enable 이 켜져도(payload 래핑) 파싱이 유지된다."""
        envelope = orjson.loads(make_message(after=make_after()))
        wrapped = orjson.dumps({"schema": {"type": "struct"}, "payload": envelope})

        event = parse_cdc_message(wrapped)
        assert event is not None
        assert event.price == Decimal("150.25")


class TestPriceDecoding:
    """Debezium decimal 인코딩 3종 + VariableScaleDecimal 구조체."""

    @pytest.mark.parametrize(
        ("wire_price", "expected"),
        [
            # 기본값: precise + JsonConverter BASE64 → base64(unscaled bytes)
            (encode_debezium_decimal("150.25"), Decimal("150.25")),
            # decimal.handling.mode=string
            ("150.25", Decimal("150.25")),
            # decimal.handling.mode=double 또는 decimal.format=NUMERIC
            (150.25, Decimal("150.25")),
            (150, Decimal("150")),
            # scale 미지정 NUMERIC 컬럼의 VariableScaleDecimal 구조체
            (
                {"scale": 2, "value": encode_debezium_decimal("150.25")},
                Decimal("150.25"),
            ),
        ],
    )
    def test_supported_encodings(self, wire_price: object, expected: Decimal) -> None:
        event = parse_cdc_message(make_message(after=make_after(price=wire_price)))
        assert event is not None
        assert event.price == expected


class TestEventTimeDecoding:
    """ZonedTimestamp(ISO 문자열)와 MicroTimestamp(µs epoch) 를 모두 지원."""

    def test_micro_epoch_int_is_decoded_exactly(self) -> None:
        expected = datetime(2026, 7, 7, 4, 5, 6, 123456, tzinfo=UTC)
        micros = (expected - _UNIX_EPOCH) // timedelta(microseconds=1)

        event = parse_cdc_message(make_message(after=make_after(event_time=micros)))
        assert event is not None
        assert event.event_time == expected

    def test_naive_timestamp_is_rejected(self) -> None:
        after = make_after(event_time="2026-07-07T04:05:06.123456")
        with pytest.raises(CdcMessageParseError):
            parse_cdc_message(make_message(after=after))


class TestIgnorePolicy:
    """실시간 시세와 무관한 메시지는 오류가 아니라 None(무시)이다."""

    @pytest.mark.parametrize("op", ["u", "d", "t", "m"])
    def test_non_insert_ops_return_none(self, op: str) -> None:
        # u 는 before/after 가 채워져 와도 정책상 무시된다.
        assert parse_cdc_message(make_message(op=op, after=make_after())) is None

    def test_tombstone_returns_none(self) -> None:
        assert parse_cdc_message(None) is None


class TestParseFailures:
    """계약 위반은 단일 예외 타입(CdcMessageParseError)으로 드러난다."""

    def test_invalid_json_raises(self) -> None:
        with pytest.raises(CdcMessageParseError):
            parse_cdc_message(b"not-json{{{")

    def test_non_object_json_raises(self) -> None:
        with pytest.raises(CdcMessageParseError):
            parse_cdc_message(b"[1, 2, 3]")

    def test_unknown_op_raises(self) -> None:
        with pytest.raises(CdcMessageParseError):
            parse_cdc_message(make_message(op="x", after=make_after()))

    def test_insert_without_after_raises(self) -> None:
        with pytest.raises(CdcMessageParseError):
            parse_cdc_message(make_message(op="c", after=None))

    @pytest.mark.parametrize(
        "bad_after",
        [
            # 검증 실패: 가격은 0 초과여야 한다
            make_after(price="-150.25"),
            # 타입 실패: 어떤 decimal 인코딩으로도 해석 불가
            make_after(price="not-a-decimal!"),
            # 검증 실패: 거래량은 0 초과여야 한다
            make_after(volume=0),
            # 필수 필드 누락
            {k: v for k, v in make_after().items() if k != "ticker"},
        ],
    )
    def test_invalid_after_image_raises(self, bad_after: dict[str, Any]) -> None:
        with pytest.raises(CdcMessageParseError):
            parse_cdc_message(make_message(after=bad_after))
