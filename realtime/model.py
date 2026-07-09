"""실시간 tick 스트림 데이터 모델과 Debezium CDC 메시지 파싱(#2).

WebSocket 요청/응답 모델(TypedDict·Pydantic)과 함께, Kafka 로 들어오는
Debezium envelope(raw bytes)를 검증된 도메인 이벤트 `CdcTradeEvent` 로
변환하는 로직을 담는다. 소비 시점 호출자는 `messaging.consumer` 이며,
의존 방향은 messaging → realtime.model 단방향이다.
"""

import base64
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Annotated, Any, Literal, TypedDict

import orjson
from pydantic import (
    AwareDatetime,
    BaseModel,
    BeforeValidator,
    Field,
    ValidationError,
)


class TickUpdate(TypedDict):
    ticker: str
    tick: int


class TickData(TypedDict):
    open: float
    high: float
    low: float
    close: float
    volume: int


type TickStreamStatus = Literal["ok", "empty", "stale"]


class TradeHighAndLow(TypedDict):
    type: Literal["candle_tick"]
    ticker: str
    data: TickData | None
    current_tick: int
    status: TickStreamStatus
    stale: bool


class RealtimeTickUpdate(BaseModel):
    ticker: Annotated[
        str,
        Field(
            min_length=1,
            max_length=10,
            title="Stock Ticker",
            description="The stock ticker symbol",
            examples=["AAPL"],
        ),
    ]
    tick: Annotated[
        int,  # s 단위, 향후 ms 단위로 개선해야함
        Field(
            gt=0,
            le=60,  # 실시간 시세창에서 1분 이상 rate 는 의미 없음
            title="Tick Interval",
            description="The interval in seconds for data updates",
            examples=[5],
        ),
    ]


# ---------------------------------------------------------------------------
# Debezium CDC 메시지 파싱 (#2)
#
# 커넥터/워커 설정 기준의 wire format:
#   - JsonConverter + value.converter.schemas.enable=false
#     → payload 가 schema 래핑 없이 {before, after, source, op, ts_ms} 로 온다.
#   - decimal.handling.mode 기본값(precise) + JsonConverter 기본 decimal.format
#     → DECIMAL(12,2) 가 unscaled big-endian bytes 의 base64 문자열로 온다.
#   - event_time TIMESTAMPTZ → io.debezium.time.ZonedTimestamp(ISO-8601 문자열).
# ---------------------------------------------------------------------------

# stock_trades.price 는 DECIMAL(12, 2). schemas.enable=false 로 스키마가 제거되면
# base64 payload 에 scale 정보가 함께 오지 않으므로, DDL(01-ddl.sql)이 정의한
# scale(2)을 여기서 계약으로 고정한다.
_PRICE_DECIMAL_SCALE = 2

_UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

# Debezium envelope 의 op 코드: c=insert, r=snapshot read, u=update, d=delete,
# t=truncate, m=logical decoding message. 이 외의 값은 계약 위반으로 본다.
type DebeziumOperation = Literal["c", "r", "u", "d", "t", "m"]

# 신규 체결로 취급해 파싱하는 op. 실시간 시세는 insert(c)와 초기 snapshot(r)만
# 의미가 있고 u/d(및 t/m)는 무시한다 — 개발 항목 #2 의 정책 결정.
_TRADE_INSERT_OPS: frozenset[str] = frozenset({"c", "r"})


class CdcMessageParseError(ValueError):
    """Debezium CDC 메시지를 도메인 이벤트로 변환하지 못했을 때의 단일 예외.

    JSON 디코딩 실패·envelope 구조 위반·`after` 필드 검증 실패를 모두 이
    타입으로 감싸, 소비 루프의 오류 처리(#6)가 예외 하나만 다루면 되게 한다.
    원인 예외는 `__cause__` 로 보존된다.
    """


def _decode_debezium_decimal(value: object) -> object:
    """Debezium 이 만들 수 있는 decimal 인코딩들을 `Decimal` 로 정규화한다.

    변환 불가능한 입력은 그대로 반환해 pydantic 이 표준 ValidationError 를
    내도록 한다(이 함수는 직접 raise 하지 않는다).

    - JSON number(`decimal.handling.mode=double` 또는 `decimal.format=NUMERIC`):
      `str()` 경유로 float 의 최단 표현("150.25")을 살려 이진 오차 확장을 피한다.
    - 십진 문자열(`decimal.handling.mode=string`): 그대로 `Decimal` 파싱.
    - base64 문자열(기본값 precise): unscaled two's-complement bytes 를 정수로
      복원한 뒤 DDL scale 을 적용한다.
    - VariableScaleDecimal 구조체(scale 미지정 NUMERIC 컬럼): {"scale", "value"}.
    """
    # bool 은 int 의 서브클래스라 아래 분기로 새지 않게 먼저 걸러낸다.
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float):
        return Decimal(str(value))
    if isinstance(value, dict):
        scale = value.get("scale")
        encoded = value.get("value")
        if not (isinstance(scale, int) and isinstance(encoded, str)):
            return value
        try:
            raw = base64.b64decode(encoded, validate=True)
        except ValueError:
            return value
        unscaled = int.from_bytes(raw, byteorder="big", signed=True)
        return Decimal(unscaled).scaleb(-scale)
    if isinstance(value, str):
        try:
            return Decimal(value)
        except InvalidOperation:
            pass
        try:
            raw = base64.b64decode(value, validate=True)
        except ValueError:
            return value
        if not raw:
            return value
        unscaled = int.from_bytes(raw, byteorder="big", signed=True)
        return Decimal(unscaled).scaleb(-_PRICE_DECIMAL_SCALE)
    return value


def _decode_debezium_timestamp(value: object) -> object:
    """Debezium temporal 표현을 timezone-aware `datetime` 으로 정규화한다.

    - int: `io.debezium.time.MicroTimestamp` — epoch 이후 마이크로초.
      timedelta 정수 산술로 float 경유 없이 마이크로초 정밀도를 보존한다.
    - str: `io.debezium.time.ZonedTimestamp` — ISO-8601(+offset). 현재 스키마의
      `event_time TIMESTAMPTZ` 가 실제로 이 형태로 오며, pydantic 의 기본
      datetime 파싱에 맡긴다(naive 문자열은 `AwareDatetime` 이 거부).
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return _UNIX_EPOCH + timedelta(microseconds=value)
    return value


type DebeziumDecimal = Annotated[Decimal, BeforeValidator(_decode_debezium_decimal)]
type DebeziumTimestamp = Annotated[
    AwareDatetime, BeforeValidator(_decode_debezium_timestamp)
]


class CdcTradeEvent(BaseModel):
    """Kafka CDC 메시지 1건에서 추출·검증된 체결 이벤트(도메인 객체).

    Debezium envelope 의 `after` 이미지를 타입 변환한 결과로, 실시간 tick
    집계(#4)가 소비할 필드 집합이다. `after` 의 나머지 컬럼(event_id,
    trade_id, currency_code)은 pydantic 기본 정책(extra ignore)으로 무시한다.
    """

    ticker: Annotated[
        str,
        Field(
            min_length=1,
            max_length=10,
            description="주식 심볼 — stock_trades.ticker VARCHAR(10)",
        ),
    ]
    price: Annotated[
        DebeziumDecimal,
        Field(
            gt=Decimal(0),
            max_digits=12,
            decimal_places=2,
            description="체결 가격 — DDL DECIMAL(12, 2) 과 동일 제약",
        ),
    ]
    event_time: Annotated[
        DebeziumTimestamp,
        Field(description="체결 시각(UTC, timezone-aware) — tick 윈도우 기준"),
    ]
    volume: Annotated[
        int,
        Field(gt=0, description="거래량 — tick 캔들 집계(#4)의 volume 합산용"),
    ]
    trade_type: Annotated[
        str,
        Field(
            min_length=1,
            max_length=10,
            description="거래 유형 — env.toml trade_types 기반 값(BUY/SELL 등)",
        ),
    ]
    market_code: Annotated[
        str | None,
        Field(max_length=10, description="시장 코드 — DDL 상 NULL 허용"),
    ] = None

    # fan-out(#3) 시 여러 WS 연결이 같은 이벤트를 공유하므로 불변으로 고정한다.
    model_config = {"frozen": True}


class DebeziumEnvelope(BaseModel):
    """`schemas.enable=false` 기준 Debezium change event 의 최상위 구조.

    도메인 변환에 필요한 최소 필드만 선언하고, 나머지(transaction, ts_us 등)는
    무시한다. `source` 는 TimescaleDb SMT 재라우팅 때문에 하이퍼테이블이 아닌
    `_timescaledb_internal` chunk 를 가리키므로 검증 대상이 아니다.
    """

    op: DebeziumOperation
    before: dict[str, Any] | None = None
    after: dict[str, Any] | None = None
    source: dict[str, Any] | None = None
    ts_ms: int | None = None


def parse_cdc_message(raw: bytes | str | None) -> CdcTradeEvent | None:
    """raw Kafka 메시지(value)를 검증된 체결 이벤트로 변환한다.

    Returns:
        CdcTradeEvent: op 가 신규 체결(`c`) 또는 snapshot 읽기(`r`)인 경우.
        None: 정책상 무시하는 메시지 — tombstone(value 없음) 및 `u`/`d`/`t`/`m`.

    Raises:
        CdcMessageParseError: JSON 이 아니거나, envelope 구조가 계약과 다르거나,
            `after` 가 도메인 검증(타입·제약)에 실패한 경우.
    """
    # Kafka tombstone(로그 컴팩션용 삭제 마커)은 value 자체가 없다 — 오류가
    # 아니라 무시 대상이다.
    if raw is None:
        return None

    try:
        decoded = orjson.loads(raw)
    except orjson.JSONDecodeError as e:
        raise CdcMessageParseError(f"CDC message is not valid JSON: {e}") from e

    # 워커의 schemas.enable 이 켜지면 {"schema": ..., "payload": envelope} 로
    # 한 겹 감싸져 온다. 설정 드리프트에 견디도록 payload 를 벗겨낸다.
    if isinstance(decoded, dict) and "op" not in decoded and "payload" in decoded:
        decoded = decoded["payload"]

    try:
        envelope = DebeziumEnvelope.model_validate(decoded)
    except ValidationError as e:
        raise CdcMessageParseError(f"Unexpected Debezium envelope: {e}") from e

    if envelope.op not in _TRADE_INSERT_OPS:
        # u/d/t/m 은 실시간 시세와 무관하므로 파싱하지 않는다(#2 정책).
        return None

    if envelope.after is None:
        raise CdcMessageParseError(
            f"Debezium '{envelope.op}' message has no 'after' image"
        )

    try:
        return CdcTradeEvent.model_validate(envelope.after)
    except ValidationError as e:
        raise CdcMessageParseError(f"Invalid trade event in 'after': {e}") from e
