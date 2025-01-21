#THIRD PARTY FILES
from fastapi import APIRouter, FastAPI

#CUSTOM MODULES
from db.tables import Employer, Job, Skills

v1_router = APIRouter()

