from __future__ import annotations

import datetime as dt
import enum
import uuid
from decimal import Decimal

from src import _library


class StarSign(enum.Enum):
    PISCES = "PISCES"
    CANCER = "CANCER"
    SCORPIO = "SCORPIO"


class CustomContext(_library.Context):
    reference: uuid.UUID
    name: str
    date_of_birth: dt.date
    star_sign: StarSign
    average_time_in_s: Decimal
    something_optional: str | None
    created_at: dt.datetime


class TestContextSerialization:
    ref_object = uuid.uuid4()

    def _set_up_object(self):
        return CustomContext(
            reference=self.ref_object,
            name="Foobar Fizz",
            date_of_birth=dt.date(1900, 5, 31),
            star_sign=StarSign.SCORPIO,
            average_time_in_s=Decimal("10.44"),
            something_optional=None,
            created_at=dt.datetime(2000, 1, 1, 0, 0),
        )

    def _set_up_expected_serialized_dict(self):
        return dict(
            reference=str(self.ref_object),
            name="Foobar Fizz",
            date_of_birth="1900-05-31",
            star_sign="SCORPIO",
            average_time_in_s="10.44",
            something_optional=None,
            created_at="2000-01-01T00:00:00",
        )

    def test_serializes(self):
        person_context = self._set_up_object()
        expected = self._set_up_expected_serialized_dict()

        result = person_context.serialize()

        assert result == expected

    def test_deserializes(self):
        serialized_dict = self._set_up_expected_serialized_dict()
        expected = self._set_up_object()

        result = CustomContext.deserialize(serialized_dict)

        assert result == expected
