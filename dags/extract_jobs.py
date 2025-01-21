#STANDARD LIBRARY FILES
import shutil
import zipfile
import json
import logging
from typing import Optional
from datetime import datetime
import os
from collections import defaultdict

#THIRD PARTY LIBRARIES
from prefect import flow, task
import requests
from prefect.logging import get_run_logger
import spacy
from spacy.matcher import PhraseMatcher
from skillNer.general_params import SKILL_DB
from skillNer.skill_extractor_class import SkillExtractor

#CUSTOM FILES
from db.tables import Employer, Job, Wages, JobSkill

'''
HELPER FUNCTIONS
'''
def _fetch_seasonal_jobs(visa_type: str, date_string: str, execution_num: int = 1) -> str:
    job_urls = {
        'h2a' : f'https://api.seasonaljobs.dol.gov/datahub-search/sjCaseData/zip/h2a/{date_string}',
        'h2b' : f'https://api.seasonaljobs.dol.gov/datahub-search/sjCaseData/zip/h2b/{date_string}'
    }
    url = job_urls[visa_type]
    try:
        #Attempt to download the file
        with requests.get(url, stream = True) as r:
            with open(f'{visa_type}.zip','wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        
        #Attempt to unzip the 
        with zipfile.ZipFile(f'{visa_type}.zip','r') as zip_ref:
            zip_ref.extractall()

        #Get file in zip
        zipfile_obj = zipfile.ZipFile(f'{visa_type}.zip')
        zipfile_list = zipfile_obj.namelist()

        #Delete zip
        os.remove(f'{visa_type}.zip')

        #Return file
        logger = get_run_logger()
        logger.info(f'Name of file in zip folder: {zipfile_list[0]}')
        return zipfile_list[0]

    except:
        #Reattempt to download the file given 
        _fetch_seasonal_jobs(visa_type, date_string, execution_num + 1)

def _load_h2a_seasonal_jobs_data(file_name: str) -> None:
    logger = get_run_logger()
    data = None
    employer_dict, job_dict, wage_dict = {}, {}, {}
    with open(file_name, 'r') as f:
        data = json.load(f)

    for x in data:
        date_submitted = datetime.strptime(x['dateSubmitted'],'%Y-%m-%dT%H:%M:%S.%fZ')
        date_accepted = datetime.strptime(x['dateAcceptanceLtrIssued'],'%Y-%m-%dT%H:%M:%S.%fZ')
        date_start = datetime.strptime(x['clearanceOrder']['jobBeginDate'],'%d-%b-%Y')
        date_end = datetime.strptime(x['clearanceOrder']['jobEndDate'], '%d-%b-%Y')
        employer_dict[x['empFein']] = Employer(
            fein = x['empFein'],
            name = x['empBusinessName'],
            address_1 = x['empAddr1'],
            address_2 = x['empAddr2'] or ' ',
            city = x['empCity'] or ' ',
            state = x['empState'] or ' ',
            zip_code = x['empPostcode'] or ' ',
            country = x['empCountry'] or ' ',
            phone_number = x['empPhone'] or ' ',
            naics_code = x['empNaics'] or ' '
        )
        job_dict[x['caseNumber']] = Job(
            case_number = x['caseNumber'],
            fein = x['empFein'],
            visa_type = 'H2A',
            title = x['clearanceOrder']['jobTitle'] or ' ',
            soc_code = x['jobSoc'] or ' ',
            description = x['clearanceOrder']['jobDuties'] or ' ',
            address_1 = x['clearanceOrder']['jobAddr1'] or ' ',
            city = x['clearanceOrder']['jobCity'] or ' ',
            state = x['clearanceOrder']['jobState'] or ' ',
            zip_code = x['clearanceOrder']['jobPostcode'] or ' ',
            begin_date = date_start,
            end_date = date_end,
            submission_date = date_submitted,
            acceptance_date = date_accepted
        )
        wage_dict[x['caseNumber']] = Wages(
            id = f'wage:{x["caseNumber"]}',
            case_number = x['caseNumber'],
            total_hours = x['clearanceOrder']['jobHoursTotal'] or 0,
            monday_hours = x['clearanceOrder']['jobHoursMon'] or 0,
            tuesday_hours = x['clearanceOrder']['jobHoursTue'] or 0,
            wednesday_hours = x['clearanceOrder']['jobHoursWed'] or 0,
            thursday_hours = x['clearanceOrder']['jobHoursThu'] or 0,
            friday_hours = x['clearanceOrder']['jobHoursFri'] or 0,
            saturday_hours = x['clearanceOrder']['jobHoursSat'] or 0,
            sunday_hours = x['clearanceOrder']['jobHoursSun'] or 0,
            amount = x['clearanceOrder']['jobWageOffer'] or 0,
            denomination = x['clearanceOrder']['jobWagePer']  or ' ',
        )

    employer_objects = [employer_dict[idx] for idx in employer_dict.keys()]
    job_objects = [job_dict[idx] for idx in job_dict.keys()]
    wage_objects = [wage_dict[idx] for idx in wage_dict.keys()]

    num_employers = len(employer_objects)
    num_jobs = len(job_objects)
    num_wages = len(wage_objects)

    logger.info(f'''
        ------------H2A Jobs Import------------
        Number of Employers: {num_employers},
        Number of Jobs: {num_jobs},
        Number of Wages: {num_wages}
    ''')

    for i in range(0, num_employers, 100):
        Employer.insert(*employer_objects[i:i+100])\
        .on_conflict(action = "DO NOTHING")\
        .run_sync()

    for i in range(0, num_jobs, 100):
        Job.insert(*job_objects[i:i+100])\
        .on_conflict(action = "DO NOTHING")\
        .run_sync()

    for i in range(0, num_wages, 100):
        Wages.insert(*wage_objects[i:i+100])\
        .on_conflict(action = "DO NOTHING")\
        .run_sync()


def _load_h2b_seasonal_jobs_data(file_name: str) -> None:
    logger = get_run_logger()
    data = None
    employer_dict, job_dict, wage_dict = {}, {}, {}
    with open(file_name, 'r') as f:
        data = json.load(f)
    for x in data:
        date_submitted = datetime.strptime(x['dateApplicationSubmitted'],'%Y-%m-%dT%H:%M:%S.%fZ')
        date_accepted = datetime.strptime(x['dateAcceptanceLtrIssued'],'%Y-%m-%dT%H:%M:%S.%fZ')
        date_start = datetime.strptime(x['tempneedStart'],'%d-%b-%Y')
        date_end = datetime.strptime(x['tempneedEnd'], '%d-%b-%Y')
        employer_dict[x['empFein']] = Employer(
            fein = x['empFein'],
            name = x['empBusinessName'],
            address_1 = x['empAddr1'],
            address_2 = x['empAddr2'] or ' ',
            city = x['empCity'] or ' ',
            state = x['empState'] or ' ',
            zip_code = x['empPostcode'] or ' ',
            country = x['empCountry'] or ' ',
            phone_number = x['empPhone'] or ' ',
            naics_code = x['empNaics'] or ' '
        )
        job_dict[x['caseNumber']] = Job(
            case_number = x['caseNumber'],
            fein = x['empFein'],
            visa_type = 'H2B',
            title = x['tempneedJobtitle'] or ' ',
            soc_code = x['tempneedSoc'] or ' ',
            description = x['jobDuties'] or ' ',
            address_1 = x['jobAddr1'] or ' ',
            city = x['jobCity'] or ' ',
            state = x['jobState'] or ' ',
            zip_code = x['jobPostcode'] or ' ',
            begin_date = date_start,
            end_date = date_end,
            submission_date = date_submitted,
            acceptance_date = date_accepted
        )
        wage_dict[x['caseNumber']] = Wages(
            id = f'wage:{x["caseNumber"]}',
            case_number = x['caseNumber'],
            total_hours = x['jobHoursTotal'] or 0,
            monday_hours = x['jobHoursMon'] or 0,
            tuesday_hours = x['jobHoursTues'] or 0,
            wednesday_hours = x['jobHoursWed'] or 0,
            thursday_hours = x['jobHoursThu'] or 0,
            friday_hours = x['jobHoursFri'] or 0,
            saturday_hours = x['jobHoursSat'] or 0,
            sunday_hours = x['jobHoursSun'] or 0,
            amount = x['wageFrom'] or 0,
            denomination = x['wagePer']  or ' ',
        )
    employer_objects = [employer_dict[idx] for idx in employer_dict.keys()]
    job_objects = [job_dict[idx] for idx in job_dict.keys()]
    wage_objects = [wage_dict[idx] for idx in wage_dict.keys()]

    num_employers = len(employer_objects)
    num_jobs = len(job_objects)
    num_wages = len(wage_objects)

    logger.info(f'''
        ------------H2B Jobs Import------------
        Number of Employers: {num_employers},
        Number of Jobs: {num_jobs},
        Number of Wages: {num_wages}
    ''')

    for i in range(0, num_employers, 100):
        Employer.insert(*employer_objects[i:i+100])\
        .on_conflict(action = "DO NOTHING")\
        .run_sync()

    for i in range(0, num_jobs, 100):
        Job.insert(*job_objects[i:i+100])\
        .on_conflict(action = "DO NOTHING")\
        .run_sync()

    for i in range(0, num_wages, 100):
        Wages.insert(*wage_objects[i:i+100])\
        .on_conflict(action = "DO NOTHING")\
        .run_sync()
    pass


def _scrape_skills_from_job_description() -> None:
    logger = get_run_logger()

    nlp = spacy.load("en_core_web_lg")
    skill_extractor = SkillExtractor(nlp, SKILL_DB, PhraseMatcher)
    skills_data = {}

    #Retrieve all unparsed jobs
    jobs_list = Job.select(Job.case_number, Job.description)\
    .where(Job.parsed_skills == False)\
    .run_sync()
    
    logger.info(f'''
        ------------Jobs Parsing------------
        Number of Jobs to Parse: {len(jobs_list)},
    ''')

    #Parse skills from each job posting
    for job in jobs_list:
        skill_count_mapping = defaultdict(int)
        skill_name_mapping = defaultdict(str)
        try:
            parsed_skills = skill_extractor.annotate(job['description'])
            for parsed_skill in parsed_skills['results'].get('full_matches',[]):
                skill_count_mapping[parsed_skill['skill_id']] += 1
                skill_name_mapping[parsed_skill['skill_id']] = parsed_skill['doc_node_value']
            for parsed_skill in parsed_skills['results'].get('ngram_scored',[]):
                skill_count_mapping[parsed_skill['skill_id']] += 1
                skill_name_mapping[parsed_skill['skill_id']] = parsed_skill['doc_node_value']

            skills_data[job['case_number']] = {
                'skills' : skill_name_mapping,
                'skill_counts' : skill_count_mapping
            }
        except:
            logger.warning(f'''
                ------------------WARNING------------------
                Job ID: {job["case_number"]}
                Job Description: {job["description"]}
                ------------------WARNING------------------
            ''')


    #Save data to JSON file
    with open('skills.json','w') as f:
        json.dump(skills_data, f, indent = 2)

    #All unparsed jobs have been parsed and saved. Update status and exit.
    Job.update({Job.parsed_skills: True})\
    .where(Job.parsed_skills == False)\
    .run_sync()


def _upload_skills_to_postgres() -> None:
    logger = get_run_logger()

    #Load skills data
    data = None
    with open('skills.json','r') as f:
        data = json.load(f)

    #Create jobskill objects
    job_skill_objects = []
    for job_id in data.keys():
        for skill_id in data[job_id]['skills'].keys():
            job_skill_objects.append(
                JobSkill(
                    id = f'{job_id}::{skill_id}',
                    job_id = job_id,
                    skill_id = skill_id,
                    skill_name = data[job_id]['skills'][skill_id],
                    frequency = data[job_id]['skill_counts'][skill_id]
                )
            )

    #Load jobskill objects to database
    num_job_skills = len(job_skill_objects)
    logger.info(f'''
        ------------------SKILLS------------------
        Number of JobSkills: {num_job_skills}
        ------------------------------------------
    ''')
    for i in range(0, num_job_skills, 100):
        JobSkill.insert(*job_skill_objects[i:i+100])\
        .on_conflict(action = "DO NOTHING")\
        .run_sync()

'''
DAG TASKS
'''
@task(name = 'fetch-h2a-jobs', 
      description = '''This task downloads the most recent H2A visa jobs from the US DOL Website''')
def fetch_h2a_seasonal_jobs():
    date_today = datetime.today().strftime('%Y-%m-%d')
    _fetch_seasonal_jobs('h2a',date_today)

@task(name = 'fetch-h2b-jobs', 
      description = '''This task downloads the most recent H2B visa jobs from the US DOL Website''')
def fetch_h2b_seasonal_jobs():
    date_today = datetime.today().strftime('%Y-%m-%d')
    _fetch_seasonal_jobs('h2b',date_today)

@task(name = 'load-h2a-jobs', 
      description = '''This task reads the downloaded H2A Jobs JSON file and 
      uploads the jobs and employers to a Postgres Database''')
def load_h2a_seasonal_jobs():
    date_today = datetime.today().strftime('%Y-%m-%d')
    file_name = f'{date_today}_h2a.json'
    _load_h2a_seasonal_jobs_data(file_name)

@task(name = 'load-h2b-jobs', 
      description = '''This task reads the downloaded H2B Jobs JSON file and 
      uploads the jobs and employers to a Postgres Database''')
def load_h2b_seasonal_jobs():
    date_today = datetime.today().strftime('%Y-%m-%d')
    file_name = f'{date_today}_h2b.json'
    _load_h2b_seasonal_jobs_data(file_name)

@task(name = 'scrape-skills',
      description = '''This task reads the job description, parses skills from the description,
      and then uploads the data to a JSON file locally.
      ''')
def parse_skills_from_description():
    _scrape_skills_from_job_description()
    pass


@task(name = 'upload-skills',
      description = '''This task uploads the skills to the PostgreSQL database.''')
def upload_skills_to_database():
    _upload_skills_to_postgres()
    pass



'''
DAG FLOW
'''
@flow
def seasonal_jobs_pipeline():
    fetch_h2a_seasonal_jobs()
    fetch_h2b_seasonal_jobs()
    load_h2a_seasonal_jobs()
    load_h2b_seasonal_jobs()
    parse_skills_from_description()
    upload_skills_to_database()


