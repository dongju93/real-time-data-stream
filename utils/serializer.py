import decimal
from typing import Any


def serialize_value(value: Any) -> str | Any:
    """직렬화가 필요한 값을 문자열로 변환"""
    if isinstance(value, decimal.Decimal) or (
        hasattr(value, "__class__") and "UUID" in value.__class__.__name__
    ):
        return str(value)
    return value
