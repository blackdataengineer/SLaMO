#THIRD PARTY FILES
from fastapi import APIRouter, FastAPI

#CUSTOM MODULES
from db.tables import Employer, Job

v1_router = APIRouter()

@v1_router.get("/jobs/", tags=["jobs"])
async def get_jobs(
    soc_code: str = None,
    limit: int = 10, 
    offset: int = 0
):
    query = Job.select()
    if soc_code != None:
        query = query.where(Job.soc_code == soc_code)
    
    return await query\
    .limit(limit)\
    .offset(offset)

@v1_router.get('/jobs/{case_number}', tags = ["employers"])
async def get_specific_job(case_number):
    return await Job.select()\
    .where(Job.case_number == case_number)\
    .first()