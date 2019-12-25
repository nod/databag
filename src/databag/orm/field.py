
import json
from datetime import datetime, timedelta, timezone


parse_date = lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%f%z")


class Field:
    _default = None
    _field_type = None
    _to_json = None
    _from_json = None
    _value = None
    _field_name = None

    def __init__(self,
            field_type,
            default=None,
            toj=None,
            fromj=None
            ):
        if default is not None: self._default = default
        # need toj fromj
        self._field_type = field_type
        self._default = default
        self._from_json = fromj or field_type

    def set_field_name(self, name):
        self._field_name = name

    def __set__(self, instance, value):
        field_name = self._field_name
        if value is None and self._default:
            value = (
                self._default() if callable(self._default)
                else self._default
                )
        if not isinstance(value, self._field_type):
            raise ValueError("invalid type")
        instance[field_name] = value

    def __get__(self, obj, cls=None):
        if obj is None: # cls object gets the field, not val
            return self
        # can we just get the value from the model instance's value store?
        val = obj[self._field_name]
        # if the value was None, is there a default?
        if val is None and self._default is not None:
            val = (
                self._default() if callable(self._default)
                else self._default
                )
            # first time we've asked for default, save it on the model instance
            obj[self._field_name] = val
        return val


# ########################
# convenience fields
# ########################


class StrField(Field):
    def __init__(self, default=''):
        super(StrField, self).__init__(str, default=default)


class IntField(Field):
    def __init__(self, default=0):
        super(IntField, self).__init__(int, default=default)


class DateTimeField(Field):
    def __init__(self, default=lambda:datetime.now(timezone.utc)):
        super(DateTimeField, self).__init__(
            field_type=datetime,
            default=default,
            toj=lambda x: x.isoformat(),
            fromj=parse_date
            )

