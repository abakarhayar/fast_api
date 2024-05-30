from fastapi import FastAPI, HTTPException
from connect_bd import get_metadata

app = FastAPI()

@app.on_event("shutdown")
def shutdown_event():
    db_conn.close()

@app.get("/olympic_hosts")
async def get_all_olympic_hosts():
    try:
        metadata = get_metadata()
        olympic_hosts = metadata["olympic_hosts"]
        session = metadata["session"]
        
        query = session.query(olympic_hosts).limit(10)  # Limiter à 10 résultats
        result = [row._asdict() for row in query]
        session.close()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/olympic_athletes")
async def get_all_olympic_athletes():
    try:
        metadata = get_metadata()
        olympic_athletes = metadata["olympic_athletes"]
        session = metadata["session"]
        
        query = session.query(olympic_athletes).limit(10)  # Limiter à 10 résultats
        result = [row._asdict() for row in query]
        session.close()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
