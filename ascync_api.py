from fastapi import FastAPI
from pydantic import BaseModel
import asyncpg
import aioredis

app = FastAPI()

db_pool = None

async def create_db_pool():
    global db_pool
    db_pool = await asyncpg.create_pool(
        database="metadata_db",
        user="postgres",
        password="123456",
        host="localhost",
    )

@app.on_event("startup")
async def startup_db_pool():
    await create_db_pool()

@app.on_event("shutdown")
async def shutdown_db_pool():
    await db_pool.close()

# Redis connection pool
redis_pool = None

async def create_redis_pool():
    global redis_pool
    redis_pool = await aioredis.create_pool("redis://localhost")

@app.on_event("startup")
async def startup_redis_pool():
    await create_redis_pool()

@app.on_event("shutdown")
async def shutdown_redis_pool():
    redis_pool.close()
    await redis_pool.wait_closed()

class MetadataEvent(BaseModel):
    id: int
    entity_id: int
    event_type: str
    metadata_info: str

class SimpleMetadataEvent(BaseModel):
    entity_id: int
    event_type: str
    metadata_info: str

@app.get("/metadata_events/{event_id}", response_model=MetadataEvent)
async def read_metadata_event(event_id: int):
    async with db_pool.acquire() as conn:
        query = """
        SELECT id, entity_id, event_type, metadata_info 
        FROM metadata_events 
        WHERE entity_id = $1
        """
        event_data = await conn.fetchrow(query, event_id)
    
    if event_data:
        return event_data
    
    return {"message": "Event not found"}

@app.get("/metadata_events/", response_model=list[SimpleMetadataEvent])
async def read_metadata_events(skip: int = 0, limit: int = 100):
    async with db_pool.acquire() as conn:
        query = """
        SELECT entity_id, event_type, metadata_info 
        FROM metadata_events 
        LIMIT $1 OFFSET $2
        """
        events_data = await conn.fetch(query, limit, skip)
    
    return events_data

@app.get("/metadata_events_all/", response_model=list[SimpleMetadataEvent])
async def read_metadata_events(skip: int = 0):
    async with db_pool.acquire() as conn:
        query = """
        SELECT entity_id, event_type, metadata_info 
        FROM metadata_events 
        OFFSET $1
        """
        events_data = await conn.fetch(query, skip)
    
    return events_data

@app.get("/latest_metadata_event", response_model=MetadataEvent)
async def read_latest_metadata_event():
    async with db_pool.acquire() as conn:
        query = """
        SELECT id, entity_id, event_type, metadata_info 
        FROM metadata_events 
        ORDER BY id DESC
        LIMIT 1
        """
        event_data = await conn.fetchrow(query)
    
    if event_data:
        return event_data
    
    return {"message": "No events found"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
