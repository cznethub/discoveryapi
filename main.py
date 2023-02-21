from datetime import datetime
from functools import lru_cache
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel
import uvicorn
import pandas
import tempfile
import json
from os.path import join


dotenv_file = '.env'

class Settings(BaseModel):
    connection_string: str = "mongodb+srv://zoya:3t0ag8xuYFquioQz@cluster0.iouzjvv.mongodb.net/?retryWrites=true&w=majority"
    database_name: str = "czo"

    class Config:
        env_file = dotenv_file

@lru_cache()
def get_settings():
    return Settings()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_db_client():
    settings = get_settings()
    app.mongodb_client = AsyncIOMotorClient(settings.connection_string)
    app.mongodb = app.mongodb_client[settings.database_name]
    clusters = await app.mongodb["cznet"].find().distinct('clusters')
    app.clusters = clusters


@app.on_event("shutdown")
async def shutdown_db_client():
    app.mongodb_client.close()

@app.get("/search")
async def search(request: Request, term: str, sortBy: str = None, contentType: str = None, providerName: str = None, creatorName: str = None, dataCoverageStart: int = None, dataCoverageEnd: int = None, publishedStart: int = None, publishedEnd: int = None, pageNumber: int = 1, pageSize: int = 30):

    searchPaths = ['name', 'description', 'keywords']
    highlightPaths = ['name', 'description', 'keywords', 'creator.@list.name']
    autoCompletePaths = ['name', 'description', 'keywords']
    
    should = [{'autocomplete': {'query': term, 'path': key, 'fuzzy': {'maxEdits': 1}}} for key in autoCompletePaths]     
    must = []
    stages = []
    filters = []
    
    if publishedStart:
        filters.append({
            'range': {
            'path': 'datePublished',
            'gte': datetime(publishedStart, 1, 1),
        },
    })
    
    if publishedEnd:
        filters.append({
            'range': {
            'path': 'datePublished',
            'lt': datetime(publishedEnd + 1, 1, 1) # +1 to include all of the publishedEnd year
        },
    })
    
    if dataCoverageStart:
        filters.append({
            'range': {
                'path': 'temporalCoverage.start',
                'gte': datetime(dataCoverageStart, 1, 1)
            }
        })
    
    if dataCoverageEnd:
        filters.append({
            'range': {
                'path': 'temporalCoverage.end',
                'lt': datetime(dataCoverageEnd + 1, 1, 1)
            }
        })
    
    if creatorName:
        must.append({
            'text': {
                'path': 'creator.@list.name',
                'query': creatorName
            }
        })
    
    if providerName:
        must.append({
            'text': {
                'path': 'provider.name',
                'query': providerName
            }
        })

    if contentType:
        must.append({
            'text': {
                'path': '@type',
                'query': contentType
            }
        })
    
    stages.append(
        {
            '$search': {
                'index': 'fuzzy_search',
                'compound': {
                    'filter': filters,
                    'should': should,
                    'must': must
                },
            'highlight': { 'path': highlightPaths }
        }
      }
    )
    
    # Sort needs to happen before pagination
    if sortBy:
        stages.append({
            '$sort': { 
                [sortBy]: 1
            }
        })
    
    stages.append(
      {
        '$skip': (pageNumber - 1) * pageSize
      }
    )
    stages.append(
      {
        '$limit': pageSize
      },
    )
    stages.append(
        {
            '$unset': ['_id']
        }
    )
    stages.append(
      { 
        '$set': {
          'score': { '$meta': 'searchScore' },
          'highlights': { '$meta': 'searchHighlights' }
        } 
      },
    )

    result = await request.app.mongodb["cznet"].aggregate(stages).to_list(pageSize)
    return result


@app.get("/typeahead")
async def typeahead(request: Request, term: str, pageSize: int = 30):
    autoCompletePaths = ['name', 'description', 'keywords']
    highlightsPaths = ['name', 'description', 'keywords']
    should = [{'autocomplete': {'query': term, 'path': key, 'fuzzy': {'maxEdits': 1}}} for key in autoCompletePaths]   

    stages = [
    {
        '$search': {
            'index': 'fuzzy_search', 
            'compound': {
                'should': [
                    {
                        'autocomplete': {
                            'query': term, 
                            'path': 'description', 
                            'fuzzy': {
                                'maxEdits': 1
                            }
                        }
                    },
                    {
                        'autocomplete': {
                            'query': term, 
                            'path': 'name', 
                            'fuzzy': {
                                'maxEdits': 1
                            }
                        }
                    },
                    {
                        'autocomplete': {
                            'query': term, 
                            'path': 'keywords', 
                            'fuzzy': {
                                'maxEdits': 1
                            }
                        }
                    }
                ]
            }, 
            'highlight': {
                'path': ['description', 'name', 'keywords']
            }
        }
    }, {
        '$project': {
            'name': 1, 
            'description': 1, 
            'keywords': 1, 
            'highlights': {
                '$meta': 'searchHighlights'
            }, 
            '_id': 0
        }
    }
]
    result = await request.app.mongodb["cznet"].aggregate(stages).to_list(pageSize)
    return result


@app.get("/csv")
async def sanitize(request: Request):
    project = [{
        '$project': {
            'name': 1, 
            'description': 1, 
            'keywords': 1,
            '_id': 0
        }
    }]
    json_response = await request.app.mongodb["cznet"].aggregate(project).to_list(None)
    df = pandas.read_json(json.dumps(json_response))
    filename = "file.csv"
    df.to_csv(filename)
    return FileResponse(filename, filename=filename, media_type='application/octet-stream')


@app.get("/clusters")
async def clusters(request: Request):
    return request.app.clusters


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
