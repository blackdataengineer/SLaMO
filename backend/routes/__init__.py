#THIRD PARTY FILES
from fastapi import APIRouter

#CUSTOM FILES
from .employers import v1_router as v1_employer_router
from .jobs import v1_router as v1_job_router

v1_router = APIRouter(prefix = '/api/v1')
v1_router.include_router(v1_employer_router)
v1_router.include_router(v1_job_router)