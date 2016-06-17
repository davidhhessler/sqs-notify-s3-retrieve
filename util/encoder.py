import json
from datetime import datetime


# Custom encoder so that responses with datetime values don't choke JSON encoding
class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()

        return json.JSONEncoder.default(self, o)
