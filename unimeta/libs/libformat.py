from datetime import datetime
from decimal import Decimal

def jsonity(data:dict) -> dict:
    r = {}
    for k, v in data.items():
        if isinstance(v, datetime):
            r[k] = v.isoformat()
        elif isinstance(v, Decimal):
            r[k] = str(v)
        else:
            r[k] = v
    return r