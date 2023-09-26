
import sys
sys.path.insert(0, '.\\py-linkedin-jobs-scraper\\')

from linkedin_jobs_scraper import LinkedinScraper
from linkedin_jobs_scraper.events import Events, EventData, EventMetrics
from linkedin_jobs_scraper.query import Query, QueryOptions, QueryFilters
from linkedin_jobs_scraper.filters import RelevanceFilters, TimeFilters, TypeFilters, ExperienceLevelFilters, \
    OnSiteOrRemoteFilters

from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from shared import BaseJob, path_to_db
from datetime import datetime, timedelta
from sqlalchemy.dialects import sqlite

import argparse

# Parameters to choose
fail_limit = 600
limit = 1000
time_delta = timedelta(weeks=2)

Base = declarative_base()

class Job(BaseJob):
    def is_recorded_in_current_session(self):
        session = BaseJob.get_handler()
        query = session.query(Job).filter_by(
            session = self.session,
            #description = self.description,
            location = self.location)
        print(self.session)
        print(len(self.description))
        print(self.location)
        print(query.statement.compile(dialect=sqlite.dialect()))
        result = session.query(Job).filter_by(
            session = self.session,
            description = self.description,
            location = self.location).first() is not None
        session.close()
        return result
    
    def set_repeated_job_id(self):
        session = BaseJob.get_handler()
        global time_delta
        time_threshold = datetime.now() - time_delta
        result = session.query(Job).filter(
            Job.timestamp >= time_threshold,
            Job.description == self.description,
            Job.location == self.location
        ).first()
        session.close()
        self.duplicationId = 0
        if result:
            self.duplicationId = result.id
            self.filteredReason = "Duplicant"

    def flush(self):
        global sessionId
        self.session = sessionId
        session = BaseJob.get_handler()
        session.add(self)
        session.commit()
        session.close()
    
def create_job(data: EventData) -> Job:
    city = data.place.split(',')[0]
    global sessionId
    return Job(title=data.title,
            company=data.company,
            location=data.location,
            city=city,
            tags=str(data.insights),
            link=data.link,
            description = data.description.replace("About the job", ""), # hack to remove obligatory "the" from non English job description          
            session=sessionId)

class MyMetrics():
    total : int = 0
    invalid_job : int = 0
    already_added : int = 0
    success : int = 0
    flush_failed : int = 0
    failed_in_row: int = 0

    def report_fail(self):
        self.total += 1
        self.failed_in_row += 1
        global isRepeat
        if self.invalid_job > fail_limit:
            print('[ON_DATA]', 'No new jobs on data')
            isRepeat = False
            sys.exit()

    def report_invalid(self):
        print('-------- job is invalid ---------')
        self.invalid_job += 1
        self.report_fail()
    
    def report_already_added(self):
        print('-------- job allready was in this session ---------')
        self.already_added += 1
        self.report_fail()

    def report_flush_failed(self):
        print('-------- fail during flush to database ---------')
        self.flush_failed += 1
        self.report_fail()
    
    def report_success(self):
        self.success += 1
        self.total += 1
        self.failed_in_row = 0

    def __str__(self):
        return f"[total: {self.total}, invalid: {self.invalid_job}, already: {self.already_added}, success: {self.success}, no_flush: {self.flush_failed}, bad_streak {self.failed_in_row}]"

parser = argparse.ArgumentParser()
parser.add_argument('-new', action='store_true', default=False)
parser.add_argument('-dummy', action='store_true', default=False)
parser.add_argument('-locations', nargs='+', type=str, required=True)
parser.add_argument('-page', type=int, default=0)
parser.add_argument('-week', action='store_true', default=False)
parser.add_argument('-show', action='store_true', default=False)
args = parser.parse_args()
print(args)

# Setup sessionId
sessionId = Job.get_last_session()
if args.new:
    sessionId += 1
    print(f'New session.')
print(f'Session id: {sessionId}')

# Parsing logic here
myMetrics = MyMetrics()

def on_data(data: EventData):
    global myMetrics
    job = create_job(data)
    job.log()
    if not job.is_valid():
        myMetrics.report_invalid()
        return
    if job.is_recorded_in_current_session():
        print(f'Duplicated')
        myMetrics.report_already_added()
        return
    print(f'Not duplicated')
    job.set_repeated_job_id()
    try:
        if args.dummy:
            print(f'Dummy. Report success.')
        else:
            job.flush()
        myMetrics.report_success()
    except Exception as e:
        myMetrics.report_flush_failed()
        print(f'[ON_DATA] Exception occured: {e}')

# Fired once for each page (25 jobs)
def on_metrics(metrics: EventMetrics):
    global current_page, starting_page, myMetrics
    print('[ON_METRICS]', str(metrics), str(myMetrics))
    current_page = starting_page + (metrics.failed + metrics.missed + metrics.processed + metrics.skipped) // 25
    print('[ON_METRICS] Current page:', current_page)

def on_error(error):
    print('[ON_ERROR]', error)

prevCount = 0
def on_end():
    print('[ON_END]')
    global prevCount, myMetrics, isRepeat
    print('[ON_END] prevCount:', prevCount, ' total:', myMetrics.total)
    if prevCount == myMetrics.total:
        isRepeat = False
    prevCount = myMetrics.total

def on_begin(data):
    print('[ON_BEGIN]', data.job_total)


print(args.show)
scraper = LinkedinScraper(
    chrome_executable_path=None,  # Custom Chrome executable path (e.g. /foo/bar/bin/chromedriver) 
    chrome_options=None,  # Custom Chrome options here
    headless=not args.show,  # Overrides headless mode only if chrome_options is None
    max_workers=1,  # How many threads will be spawned to run queries concurrently (one Chrome driver for each thread)
    slow_mo=1,  # Slow down the scraper to avoid 'Too many requests 429' errors (in seconds)
    page_load_timeout=20  # Page load timeout (in seconds)    
)

# Add event listeners
scraper.on(Events.DATA, on_data)
scraper.on(Events.ERROR, on_error)
scraper.on(Events.END, on_end)
scraper.on(Events.METRICS, on_metrics)
scraper.on(Events.BEGIN, on_begin)

time = TimeFilters.WEEK if args.week else TimeFilters.DAY
current_page = args.page   
starting_page = args.page

for location in args.locations:
    isRepeat = True

    while isRepeat:
        starting_page = current_page
        queries = [
            Query(
                query='Software Engineer',
                options=QueryOptions(
        #            locations=['Ireland', 'Sweden', 'United Kingdom', 'Netherlands', "Oslo"],
                    locations=[location],
                    apply_link=False,  # Try to extract apply link (easy applies are skipped). If set to True, scraping is slower because an additional page mus be navigated. Default to False.
                    skip_promoted_jobs=False,
                    limit=limit,
                    page_offset=current_page,
                    filters=QueryFilters(
        #                company_jobs_url='https://www.linkedin.com/jobs/search/?f_C=1441%2C17876832%2C791962%2C2374003%2C18950635%2C16140%2C10440912&geoId=92000000',  # Filter by companies.                
                        relevance=RelevanceFilters.RECENT,
                        time=time,
                        type=[TypeFilters.FULL_TIME],
                        on_site_or_remote=[OnSiteOrRemoteFilters.ON_SITE, OnSiteOrRemoteFilters.HYBRID],
                        experience=[ExperienceLevelFilters.MID_SENIOR]
                    )
                )
            ),
        ]
        scraper.run(queries)
    current_page = 0  
    starting_page = 0
    print(str(myMetrics))

    if args.dummy:
        print(f'This is a dummy session')
    else:
        print(f'Session id: {sessionId}')
    