from __future__ import annotations

import datetime as dt
import decimal
import enum
import inspect
import uuid
from typing import Any, Dict, List, Sequence


def serialize(payload: dict) -> dict:
    return {k: _serialize_value(v) for k, v in payload.items()}


def deserialize(serialized_dict: dict, annotations: dict) -> dict:
    deserialized_kwargs = dict()

    for k, v in serialized_dict.items():
        if k in annotations:
            deserialized_kwargs[k] = _deserialize_value(v, annotations[k], annotations)
        else:
            deserialized_kwargs[k] = v

    return deserialized_kwargs


# Helpers


def _serialize_value(value: Any) -> Any:
    if isinstance(value, Dict):
        return serialize(value)
    elif isinstance(value, List):
        return [_serialize_value(i) for i in value] if value else []
    elif isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()
    elif isinstance(value, enum.Enum):
        return value.value
    elif isinstance(value, (uuid.UUID, decimal.Decimal)):
        return str(value)
    else:
        return value


def _deserialize_value(
    value: Any,
    annotation: type,
    annotations_: dict[str, type],
) -> Any:
    if annotation in (Dict, dict):
        return deserialize(value, annotations_)
    elif annotation in (List, list, Sequence):
        return [_deserialize_value(i, annotation, annotations_) for i in value] if value else []
    elif annotation == dt.datetime:
        return dt.datetime.fromisoformat(value)
    elif annotation == dt.date:
        return dt.date.fromisoformat(value)
    elif annotation == dt.time:
        return dt.time.fromisoformat(value)
    elif inspect.isclass(annotation) and issubclass(annotation, enum.Enum):
        return annotation(value)
    elif annotation in (decimal.Decimal, uuid.UUID):
        return annotation(value)
    else:
        return value
