import asyncio
import json
from typing import Set, Dict, List, Any
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Body
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Float,
    DateTime,
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select, insert, update, delete
from datetime import datetime
from pydantic import BaseModel, field_validator
from config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)


# SQLAlchemy setup
DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)
metadata = MetaData()
# Define the ProcessedAgentData table
processed_agent_data = Table(
    "processed_agent_data",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("road_state", String),
    Column("user_id", Integer),
    Column("x", Float),
    Column("y", Float),
    Column("z", Float),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("timestamp", DateTime),
)
SessionLocal = sessionmaker(bind=engine)


# SQLAlchemy model
class ProcessedAgentDataInDB(BaseModel):
    id: int
    road_state: str
    user_id: int
    x: float
    y: float
    z: float
    latitude: float
    longitude: float
    timestamp: datetime


# FastAPI models
class AccelerometerData(BaseModel):
    x: float
    y: float
    z: float


class GpsData(BaseModel):
    latitude: float
    longitude: float


class AgentData(BaseModel):
    user_id: int
    accelerometer: AccelerometerData
    gps: GpsData
    timestamp: datetime

    @classmethod
    @field_validator("timestamp", mode="before")
    def check_timestamp(cls, value):
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(value)
        except (TypeError, ValueError):
            raise ValueError(
                "Invalid timestamp format. Expected ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ)."
            )


class ProcessedAgentData(BaseModel):
    road_state: str
    agent_data: AgentData



# FastAPI app setup
app = FastAPI()

# WebSocket subscriptions
subscriptions: Dict[int, Set[WebSocket]] = {}


# FastAPI WebSocket endpoint
@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: int):
    await websocket.accept()
    if user_id not in subscriptions:
        subscriptions[user_id] = set()
    subscriptions[user_id].add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        subscriptions[user_id].remove(websocket)


# Function to send data to subscribed users
async def send_data_to_subscribers(user_id: int, data):
    if user_id in subscriptions:
        for websocket in subscriptions[user_id]:
            await websocket.send_json(json.dumps(data))

class ProcessedAgentDataCRUD:
    @staticmethod
    async def create(data: List[ProcessedAgentData]):
        try:
            db = SessionLocal()
            for item in data:
                road_state = item.road_state
                agent_data = item.agent_data

                user_id = agent_data.user_id
                accelerometer = agent_data.accelerometer
                gps = agent_data.gps
                timestamp = agent_data.timestamp

                query = insert(processed_agent_data).values(
                    road_state=road_state,
                    user_id=user_id,
                    x=accelerometer.x,
                    y=accelerometer.y,
                    z=accelerometer.z,
                    latitude=gps.latitude,
                    longitude=gps.longitude,
                    timestamp=timestamp
                )
                db.execute(query)

                await send_data_to_subscribers(item.agent_data.user_id, item.json())

            db.commit()
        except Exception as e:
            db.rollback()
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            db.close()
        return {"message": "Created successfully"}

    @staticmethod
    def read(processed_agent_data_id: int):
        try:
            db = SessionLocal()
            query = select(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)
            data = db.execute(query).fetchone()
            if data is None:
                raise HTTPException(status_code=404, detail="Not found")
            return data
        finally:
            db.close()

    @staticmethod
    def list():
        try:
            db = SessionLocal()
            query = select(processed_agent_data)
            data = db.execute(query).fetchall()
            return data
        finally:
            db.close()

    @staticmethod
    def update(processed_agent_data_id: int, data: ProcessedAgentData):
        try:
            db = SessionLocal()
            road_state = data.road_state
            agent_data = data.agent_data

            user_id = agent_data.user_id
            accelerometer = agent_data.accelerometer
            gps = agent_data.gps
            timestamp = agent_data.timestamp
            update_query = (
                update(processed_agent_data)
                .where(processed_agent_data.c.id == processed_agent_data_id)
                .values(
                    road_state=road_state,
                    user_id=user_id,
                    x=accelerometer.x,
                    y=accelerometer.y,
                    z=accelerometer.z,
                    latitude=gps.latitude,
                    longitude=gps.longitude,
                    timestamp=timestamp
                )
            )

            db.execute(update_query)
            db.commit()
            updated_data = db.execute(select(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)).fetchone()
            return updated_data
        except Exception as e:
            db.rollback()
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            db.close()

    @staticmethod
    def delete(processed_agent_data_id: int):
        try:
            db = SessionLocal()

            data_to_delete = db.execute(select(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)).fetchone()

            if data_to_delete is None:
                raise HTTPException(status_code=404, detail="Not found")
            delete_query = delete(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)
            db.execute(delete_query)
            db.commit()
            return data_to_delete
        except Exception as e:
            db.rollback()
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            db.close()

# FastAPI CRUD endpoints
@app.post("/processed_agent_data/")
async def create_processed_agent_data(data: List[ProcessedAgentData]):
    return await ProcessedAgentDataCRUD.create(data)

@app.get(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB,
)
def read_processed_agent_data(processed_agent_data_id: int):
    return ProcessedAgentDataCRUD.read(processed_agent_data_id)

@app.get("/processed_agent_data/", response_model=list[ProcessedAgentDataInDB])
def list_processed_agent_data():
    return ProcessedAgentDataCRUD.list()

@app.put(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB,
)
def update_processed_agent_data(processed_agent_data_id: int, data: ProcessedAgentData):
    return ProcessedAgentDataCRUD.update(processed_agent_data_id, data)

@app.delete(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB,
)
def delete_processed_agent_data(processed_agent_data_id: int):
    return ProcessedAgentDataCRUD.delete(processed_agent_data_id)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
