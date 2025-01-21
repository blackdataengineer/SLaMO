from piccolo.table import Table
from piccolo.columns import Varchar
from piccolo.columns import Integer
from piccolo.columns import ForeignKey
from piccolo.columns import Text
from piccolo.columns import Date
from piccolo.columns import Boolean

class Employer(Table, tablename = "employers"):
	#h2b: empFein, h2a: empFein 
	fein = Varchar(length = 10, primary_key = True)
	#h2b: empBusinessName, h2a: empBusinessName
	name = Varchar(length = 255, nullable = True)
	#h2b: empAddr1, h2a: empAddr1
	address_1 = Varchar(length = 255, nullable = True)
	#h2b: empAddr2, h2a: empAddr2
	address_2 = Varchar(length = 255, nullable = True, default = "")
	#h2b: empCity, h2a: empCity
	city = Varchar(length = 255, nullable = True)
	#h2b: empState, h2a: empState
	state = Varchar(length = 16, nullable = True)
	#h2b: empPostcode, h2a: empPostcode
	zip_code = Varchar(length = 10, nullable = True)
	#h2b: empCountry, h2a: empCountry
	country = Varchar(length = 255, nullable = True)
	#h2b: empPhone, h2a: empPhone
	phone_number = Varchar(length = 15, nullable = True)
	#h2b: empNaics, h2a: empNaics
	naics_code = Varchar(length = 10, nullable = True)


class Job(Table, tablename = "jobs"):
	#h2b: caseNumber, h2a: caseNumber
	case_number = Varchar(length = 63, primary_key = True)
	#h2b: empFein, h2a: empFein
	fein = ForeignKey(references = Employer)
	#h2b: H2B, h2a: H2A
	visa_type = Varchar(length = 8, nullable = True)
	#h2b: tempneedJobtitle, h2a: clearanceOrder.jobTitle
	title = Varchar(length = 255, nullable = True)
	#h2b: tempneedSoc, h2a: jobSoc
	soc_code = Varchar(length = 10, nullable = True)
	#h2b: jobDuties, h2a: clearanceOrder.jobDuties
	description = Text()
	#h2b: jobAddr1, h2a: clearanceOrder.jobAddr1, 
	address_1 = Varchar(length = 255, nullable = True)
	#h2b: jobCity, h2a: clearanceOrder.jobCity, 
	city = Varchar(length = 255, nullable = True)
	#h2b: jobState, h2a: clearanceOrder.jobState, 
	state = Varchar(length = 10, nullable = True)
	#h2b: jobPostcode, h2a: clearanceOrder.jobPostcode, 
	zip_code = Varchar(length = 10, nullable = True)
	#h2b: tempneedStart, h2a: jobBeginDate
	begin_date = Date()
	#h2b: tempneedEnd, h2a: jobEndDate
	end_date = Date()
	#h2b: dateApplicationSubmitted, h2a: dateSubmitted
	submission_date = Date()
	#h2b: dateAcceptanceLtrIssued, h2a: dateAcceptanceLtrIssued
	acceptance_date = Date()
	parsed_skills = Boolean(default = False)


class Wages(Table, tablename = "wages"):
	id = Varchar(length = 63, primary_key = True)
	#h2b: caseNumber, h2a: caseNumber
	case_number = ForeignKey(references = Job)
	#h2b: jobHoursTotal, h2a: clearanceOrder.jobHoursTotal
	total_hours = Integer()
	#h2b: jobHoursMon, h2a: clearanceOrder.jobHoursMon
	monday_hours = Integer()
	#h2b: jobHoursTues, h2a: clearanceOrder.jobHoursTue
	tuesday_hours = Integer()
	#h2b: jobHoursWed, h2a: clearanceOrder.jobHoursWed
	wednesday_hours = Integer()
	#h2b: jobHoursThu, h2a: clearanceOrder.jobHoursThu
	thursday_hours = Integer()
	#h2b: jobHoursFri, h2a: clearanceOrder.jobHoursFri
	friday_hours = Integer()
	#h2b: jobHoursSat, h2a: clearanceOrder.jobHoursSat
	saturday_hours = Integer()
	#h2b: jobHoursSun, h2a: clearanceOrder.jobHoursSun
	sunday_hours = Integer()
	#h2b: wageFrom, h2a: clearanceOrder.jobWageOffer
	amount = Integer()
	#h2b: jobHoursTotal, h2a: clearanceOrder.jobWagePer
	denomination = Varchar(length = 32, nullable = True)


class JobSkill(Table, tablename = "job_skills"):
	id = Varchar(length = 255, primary_key = True)
	job_id = ForeignKey(references = Job)
	skill_id = Varchar(length = 255)
	skill_name = Varchar(length = 255)
	frequency = Integer()
