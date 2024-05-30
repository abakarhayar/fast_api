# app.py
from connect_bd import get_metadata
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from sqlalchemy import select, text
from sqlalchemy.orm import Session

app = FastAPI()



from typing import Dict, Optional

from fastapi import APIRouter, Depends, Query

## TABLE olympic_hosts

    

@app.get("/olympic_hosts/count")
async def get_olympic_hosts_count():
    try:
        metadata = get_metadata()
        olympic_hosts = metadata["olympic_hosts"]
        session = metadata["session"]
        tunnel = metadata["tunnel"]
        
        count_olympic_hosts = session.query(olympic_hosts).count()
        session.close()
        tunnel.stop()
        return {"count_olympic_hosts": count_olympic_hosts}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/olympic_hosts/filter_by_country")
async def filter_olympic_hosts_by_country(country_name: str):
    try:
        metadata = get_metadata()
        olympic_hosts = metadata["olympic_hosts"]
        session = metadata["session"]
        tunnel = metadata["tunnel"]
        
        query = session.query(olympic_hosts).filter(olympic_hosts.columns.game_location == country_name)
        result = [row._asdict() for row in query]
        session.close()
        tunnel.stop()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))    
    
## TABLE olympic_athletes    
@app.get("/olympic_athletes/count")
async def get_olympic_athletes_count():
    try:
        metadata = get_metadata()
        olympic_athletes = metadata["olympic_athletes"]
        session = metadata["session"]
        tunnel = metadata["tunnel"]
        
        olympic_athletes_count = session.query(olympic_athletes).count()
        session.close()
        tunnel.stop()
        return {"olympic_athletes_count": olympic_athletes_count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



def get_unique_values(db: Session, table_name: str, column_name: str):
    query = text(f"SELECT DISTINCT `{column_name}` FROM `{table_name}`")
    result = db.execute(query).fetchall()
    return [row[0] for row in result]


""" Get unique values from a column : Body request example :  {
   "table_name" : "olympic_hosts",
   "column_name" : "game_year"
}"""


@app.get("/get-unique-column/")
async def get_unique_column_values(request: Request):
    try:
        table_info : dict = await request.json()
        metadata = get_metadata()
        table_name = metadata[table_info['table_name']]
        db = metadata["session"]
        tunnel = metadata["tunnel"]
        column_name = table_info['column_name']

        # Verify if table and column exist
        table_query = text(f"SHOW TABLES LIKE '{table_name}'")
        table_exists = db.execute(table_query).first()
        if not table_exists:
            raise HTTPException(status_code=404, detail=f"Table {table_name} not found")

        column_query = text(f"SHOW COLUMNS FROM `{table_name}` LIKE '{column_name}'")
        column_exists = db.execute(column_query).first()
        if not column_exists:
            raise HTTPException(status_code=404, detail=f"Column {column_name} not found")

        unique_values = get_unique_values(db, table_name, column_name)
        db.close()
        tunnel.stop()
        return {column_name: unique_values}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    



    


def search_by_tags_repository(db: Session, table_name: str,  filters: dict):
    try:
        query = f"SELECT * FROM `{table_name}` WHERE 1=1"
        params = {}
        for key, value in filters.items():
            if value:
                query += f" AND `{key}` = :{key}"
                params[key] = value

        results = db.execute(text(query), params).mappings().fetchall()
        return results
    except Exception as e:
        raise e
# table 

# game_year
# game_season 

""" Get unique values from a column : Body request example :  {
   "table_name" : "olympic_hosts",
   "game_year" : "2002"
   game_season : "Winter"
}"""
@app.router.get("/search-tag")
async def search_by_tags_controller(
    request: Request,
    ):

    try : 
        filters = await request.json()
        metadata = get_metadata()
        table_name = metadata[filters['table_name']]
        db = metadata["session"]
        tunnel = metadata["tunnel"]
        filters_copy = filters.copy()
        del filters_copy['table_name']
        print(filters_copy)
        dto : dict =filters_copy
        print('dto', dto)
        results = search_by_tags_repository(db, table_name, dto )

        db.close()
        tunnel.stop()
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


    
def search_athletes_repository(db, table_name, athlete_full_name):
    try:
        query = text(f"SELECT * FROM `{table_name}` WHERE athlete_full_name = '{athlete_full_name}'")
        # query = select([table_name]).where(table_name.c.athlete_full_name == athlete_full_name)
        results = db.execute(query).mappings().all()
        print(results)
        return results
    except Exception as e:
        raise e

@app.router.get("/search-athletes")
def get_athletes () : 
    try : 
        metadata = get_metadata()
        olympic_athletes = metadata["olympic_athletes"]
        db = metadata["session"]
        tunnel = metadata["tunnel"]

        results = search_athletes_repository(db, olympic_athletes, "Michael Phelps")
        db.close()
        tunnel.stop()
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))