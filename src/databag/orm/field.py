
import json
from datetime import datetime, timedelta, timezone


parse_date = lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%f%z")


class Field:
    _default = None
    _nudge = None  # attempt to convert type properly
    _field_type = None
    _value = None
    _to_json = None
    _from_json = None

    def __init__(self,
            field_type,
            default=None,
            nudge=None,
            toj=None,
            fromj=None
            ):
        if default is not None: self._default = default
        # need toj fromj
        self._field_type = field_type
        self._default = default
        self._nudge = nudge

    def setval(self, value):
        if self._nudge:
            value = self._nudge(value)
        assert isinstance(value, self._field_type)
        self._value = value

    def getval(self, to_json=False):
        val = self._value
        if val is None and self._default is not None:
            val = (
                self._default() if callable(self._default)
                else self._default
                )
        if to_json and self._to_json: return self._to_json(val)
        else: return val

    def __set__(self, obj, val):
        self.setval(val)

    def __get__(self, obj, cls=None):
        if obj is None: # cls object gets the field, not val
            return self
        return self.getval()


# ########################
# convenience fields
# ########################


class IntField(Field):
    def __init__(self, default=0):
        super(IntField, self).__init__(int, default=default, nudge=int)


class DateTimeField(Field):
    def __init__(self, default=lambda:datetime.now(timezone.utc)):
        super(DateTimeField, self).__init__(
            field_type=datetime,
            default=default,
            nudge=parse_date,
            toj=lambda x: x.isoformat(),
            fromj=parse_date
            )

