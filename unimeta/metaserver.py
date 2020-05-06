from fastapi import FastAPI
import databases
import orm
import sqlalchemy
from unimeta.model import MetaEventReq
from devtools import debug
from databases import Database
import os

database_url = os.getenv("UNIMETA_DATABASE_URL", None)
database = Database(database_url)
await database.connect()


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



@app.post("/metaevent")
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
    debug(req)
    query = "SELECT * FROM notes WHERE id = :id"
    result = await database.fetch_one(query=query, values={"id": 1})

    return {"Hello": "World"}
