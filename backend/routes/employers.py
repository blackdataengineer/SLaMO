#THIRD PARTY FILES
from fastapi import APIRouter, FastAPI

#CUSTOM MODULES
from db.tables import Employer, Job

v1_router = APIRouter()

@v1_router.get("/employers/", tags=["employers"])
async def get_employers(
    naics: str = None, 
    zip_code: str = None,
    state: str = None,
    limit: int = 10, 
    offset: int = 0
):
    query = Employer.select()
    if naics != None:
        query = query.where(Employer.naics_code == naics)
    if zip_code != None:
        query = query.where(Employer.zip_code == zip_code)
    if state != None:
        query = query.where(Employer.state == state)

    return await query.limit(limit)\
    .offset(offset)

@v1_router.get('/employers/{fein}', tags = ["employers"])
async def get_specific_employer(fein):
    return await Employer.select()\
    .where(Employer.fein == fein)\
    .first()


@v1_router.get('/employers/{fein}/jobs', tags = ['employers'])
async def get_jobs_for_employer(fein):
    employer_data = Employer.select()\
    .where(Employer.fein == fein)\
    .first().run_sync()

    jobs_data = Job.select()\
    .where(Job.fein == employer_data['fein'])\
    .run_sync()

    employer_data['jobs'] = jobs_data
    return employer_data
