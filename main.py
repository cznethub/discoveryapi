from datetime import datetime
from functools import lru_cache
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel
import uvicorn
import json


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
            'gte': datetime(publishedStart),
            'lt': datetime(publishedEnd + 1) # +1 to include all of the publishedEnd year
        },
    })
    
    if dataCoverageStart:
        filters.append({
            'range': {
                'path': 'temporalCoverage.start',
                'gte': datetime(dataCoverageStart)
            }
        })
    
    if dataCoverageStart:
        filters.append({
            'range': {
                'path': 'temporalCoverage.end',
                'gte': datetime(dataCoverageEnd + 1)
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
        '$set': {
          'score': { '$meta': 'searchScore' },
          'highlights': { '$meta': 'searchHighlights' }
        } 
      },
    )

    pipeline = [
        {
            '$search': {
                'index': 'fuzzy_search', 
                'text': {
                    'query': term, 
                    'path': [
                        'description', 'name', 'keywords'
                    ]
                }
            }
        }
    ]
    result = await request.app.mongodb["cznet"].aggregate(pipeline).to_list(pageSize)
    return json.loads(json.dumps(result, default=str))


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
    return json.loads(json.dumps(result, default=str))
    


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
