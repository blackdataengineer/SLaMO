#STANDARD LIBRARY
from typing import Union

#THIRD PARTY PACKAGES
from fastapi import FastAPI

#CUSTOM FILES
from .routes import v1_router

app = FastAPI()
app.include_router(v1_router)

@app.get("/")
def read_root():
    return {"Hello": "World"}


