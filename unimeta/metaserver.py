from fastapi import FastAPI
import databases
import orm
import sqlalchemy

from pydantic import BaseModel


class MetaEventReq(BaseModel):
    name: str
    meta: str 
    creator: str


metadata = sqlalchemy.MetaData()

MetaEvent = sqlalchemy.Table(
    "meta_table",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("name", sqlalchemy.String(length=200)),
    sqlalchemy.Column("meta", sqlalchemy.JSON()),
    sqlalchemy.Column("version", sqlalchemy.Integer),
    sqlalchemy.Column("created_at", sqlalchemy.DateTime),
    sqlalchemy.Column("creator", sqlalchemy.String(length=200))
)

app = FastAPI()



@app.post("/metatable")
async def create_event_meta(req:MetaEventReq) -> dict:
    """
    @api {post} /metatable/ Create Meta Information
    @apiName GetMetaTable
    @apiGroup MetaTable

    @apiParam {String} event  event name.
    @apiParam {Object} meta meta information(avro format).
    @apiParam {String} creator meta creator.
    @apiSuccess {int} status 创建状态.
"""
    return {"Hello": "World"}


@app.get("/metas/{event_name}")
def read_item(event_name: str):
    return {"item_id": item_id, "q": q}