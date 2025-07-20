"""
Validation mixins for common field validations
"""

from typing import Any

from pydantic import field_validator


class UppercaseAlphabetValidationMixin:
    """Mixin providing validation for uppercase alphabet-only fields."""

    @classmethod
    def _validate_uppercase_alphabet(cls, value: Any) -> Any:
        """Validate that value contains only uppercase English letters."""
        if value is None:
            return value
        if not isinstance(value, str):
            raise ValueError("Must be a string")
        if not value.isalpha() or not value.isupper():
            raise ValueError("Must be uppercase English letters only")
        return value

    @field_validator("ticker", "market_code")
    @classmethod
    def validate_ticker(cls, value: Any) -> Any:
        """Validate ticker symbol format."""
        return cls._validate_uppercase_alphabet(value)
