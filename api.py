from fastapi import FastAPI
from pydantic import BaseModel
import psycopg2

app = FastAPI()

# PostgreSQL connection parameters
db_params = {
    "host": "localhost",  
    "database": "metadata_db",  
    "user": "postgres",  
    "password": "123456"  
}

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
def read_metadata_event(event_id: int):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    query = """
    SELECT id, entity_id, event_type, metadata_info 
    FROM metadata_events 
    WHERE entity_id = %s
    """
    cursor.execute(query, (str(event_id),))
    event_data = cursor.fetchone()
    
    cursor.close()
    conn.close()
    
    if event_data:
        return {
            "id": event_data[0],
            "entity_id": event_data[1],
            "event_type": event_data[2],
            "metadata_info": event_data[3],
        }
    
    return {"message": "Event not found"}

@app.get("/metadata_events/", response_model=list[SimpleMetadataEvent])
def read_metadata_events(skip: int = 0, limit: int = 100):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    query = """
    SELECT entity_id, event_type, metadata_info 
    FROM metadata_events 
    LIMIT %s OFFSET %s
    """
    cursor.execute(query, (limit, skip))
    events_data = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    events = [{"entity_id": e[0], "event_type": e[1], "metadata_info": e[2]} for e in events_data]
    
    return events

@app.get("/metadata_events_all/", response_model=list[SimpleMetadataEvent])
def read_metadata_events(skip: int = 0):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    query = """
    SELECT entity_id, event_type, metadata_info 
    FROM metadata_events 
    OFFSET %s
    """

    cursor.execute(query, (skip,))
    events_data = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    events = [{"entity_id": e[0], "event_type": e[1], "metadata_info": e[2]} for e in events_data]
    
    return events

@app.get("/latest_metadata_event", response_model=MetadataEvent)
def read_latest_metadata_event():
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    query = """
    SELECT id, entity_id, event_type, metadata_info 
    FROM metadata_events 
    ORDER BY id DESC
    LIMIT 1
    """
    cursor.execute(query)
    event_data = cursor.fetchone()
    
    cursor.close()
    conn.close()
    
    if event_data:
        return {
            "id": event_data[0],
            "entity_id": event_data[1],
            "event_type": event_data[2],
            "metadata_info": event_data[3],
        }
    
    return {"message": "No events found"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
