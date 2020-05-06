from pydantic import BaseModel


class MetaEventReq(BaseModel):
    name: str
    meta: str 
    creator: str

