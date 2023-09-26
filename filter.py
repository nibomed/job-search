from shared import BaseJob
import argparse
from sqlalchemy import update, not_, and_, text, func
import csv
from datetime import datetime, timedelta

parser = argparse.ArgumentParser()
parser.add_argument('-session', type=int, default=100000)
args = parser.parse_args()

sessionId = min(args.session, BaseJob.get_last_session())
print(f'Session id: {sessionId}')

def wrap_up(message, stmt):
    session = BaseJob.get_handler()
    result = session.execute(stmt)
    session.commit()
    print(f'{result.rowcount} rows is filtered because "{message}"')
    session.close()

def filter_out_title_with_substring(substring):
    message = f"Have '{substring}' substring in title"  
    stmt = update(BaseJob).where(
        (BaseJob.session == sessionId) &
        (BaseJob.title.ilike(f'%{substring}%')) &
        (BaseJob.filteredReason == '')
    ).values(filteredReason=f"Have '{substring}' substring in title")
    wrap_up(message, stmt)

def filter_out_title_with_word(word):
    message = f"Have '{word}' word in title"
    stmt = update(BaseJob).where(
        (BaseJob.session == sessionId) &
        (BaseJob.title.op('REGEXP')(rf'(?i)\b{word}\b')) &
        (BaseJob.filteredReason == '')
    ).values(filteredReason=f"Have '{word}' word in title")
    wrap_up(message, stmt)


def filter_out_description_with_substring(substring):
    message = f"Have '{substring}' substring in description"
    stmt = update(BaseJob).where(
        (BaseJob.session == sessionId) &
        (BaseJob.description.ilike(f'%{substring}%')) &
        (BaseJob.filteredReason == '')
    ).values(filteredReason=f"Have '{substring}' substring in description")
    wrap_up(message, stmt)

def filter_out_description_with_word(word):
    message = f"Have '{word}' word in description"
    stmt = update(BaseJob).where(
        (BaseJob.session == sessionId) &
        (BaseJob.description.op('REGEXP')(rf'(?i)\b{word}\b')) &
        (BaseJob.filteredReason == '')
    ).values(filteredReason=f"Have '{word}' word in description")
    wrap_up(message, stmt)

def filter_out_description_without_word(word):
    message = f"Have no '{word}' substring in description"
    stmt = update(BaseJob).where(
        (BaseJob.session == sessionId) &
        #(not_(BaseJob.description.ilike(f'%{substring}%'))) &
        (not_(BaseJob.description.op('REGEXP')(rf'(?i)\b{word}\b'))) &
        (BaseJob.filteredReason == "")
    ).values(filteredReason=message)
    wrap_up(message, stmt)

def filter_out_description_without_substring(substring):
    message = f"Have no '{substring}' substring in description"
    stmt = update(BaseJob).where(
        (BaseJob.session == sessionId) &
        (not_(BaseJob.description.ilike(f'%{substring}%'))) &
        (BaseJob.filteredReason == "")
    ).values(filteredReason=message)
    wrap_up(message, stmt)

def filter_out_company(word):
    message = f"Company '{word}'"
    stmt = update(BaseJob).where(
        (BaseJob.session == sessionId) &
        (BaseJob.company == word) &
        (BaseJob.filteredReason == '')
    ).values(filteredReason=f"Have '{word}' word in title")
    wrap_up(message, stmt)


def clean_filtered_reason():
    session = BaseJob.get_handler()
    stmt = update(BaseJob).where(
        BaseJob.session == sessionId
    ).values(
        filteredReason=""
    )
    session.execute(stmt)
    session.commit()
    session.close()

def export_to_csv():
    session = BaseJob.get_handler()
    rows = session.query(BaseJob).filter(
        BaseJob.session == sessionId,
        BaseJob.filteredReason == '',
        BaseJob.duplicationId == 0
        ).order_by(BaseJob.location, BaseJob.company).all()
    filename = 'result/' + str(sessionId) + '_' +datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '.csv'
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['id', 'session', 'location', 'city', 'company', 'title', 'link', 'comment', 'tags']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({
                'id': row.id,
                'session': row.session,
                'location': row.location,
                'city': row.city,
                'company': row.company,
                'title': row.title,
                'link': row.link,
                'comment': ' ',
                'tags': row.tags})
    session.close()
    print(f'Result at {filename}')
    print(len(rows))


#clean_filtered_reason()

filter_out_description_without_substring('C++')

filter_out_description_with_word('cryptocurrency')
filter_out_description_with_word('blockchain')
filter_out_description_with_word("bank")
filter_out_description_with_word("trading")
filter_out_description_with_word("finance")
filter_out_description_with_word("security Clearance")
filter_out_description_with_word("SC clearance")
filter_out_description_with_word("betting") 
filter_out_description_with_word("hedge fund")
filter_out_description_with_word("my client")
filter_out_description_with_word("Dutch language")
filter_out_description_with_word("ein")

filter_out_title_with_substring("Manager")
filter_out_title_with_substring("devops")
filter_out_title_with_substring("Database Administrator")
filter_out_title_with_substring("Network Engineer")
filter_out_title_with_substring("Quality Technician")
filter_out_title_with_substring("Machine Learning")
filter_out_title_with_substring("GPU Software")
filter_out_title_with_substring("Maintenance")
filter_out_title_with_substring("Team Lead")
filter_out_title_with_substring("Android")
filter_out_title_with_substring("IOS")
filter_out_title_with_substring("Architect")
filter_out_title_with_substring("AWS")
filter_out_title_with_substring("build")
filter_out_title_with_substring("Java")
filter_out_title_with_substring("Azure")
filter_out_title_with_substring("PHP")
filter_out_title_with_substring("Python")
filter_out_title_with_substring("Full-stack")
filter_out_title_with_substring("test")
filter_out_title_with_substring("embedded")
filter_out_title_with_substring("testing")
filter_out_title_with_substring("frontend")

filter_out_title_with_word("intern")
filter_out_title_with_word("Oracle")
filter_out_title_with_word("Full stack")
filter_out_title_with_word("game")
filter_out_title_with_word("games")
filter_out_title_with_word("mobile")
filter_out_title_with_word("Unreal")
filter_out_title_with_word("Integration")
filter_out_title_with_word("Gameplay")
filter_out_title_with_word("Machine")
filter_out_title_with_word("Learning")
filter_out_title_with_word("Go")
filter_out_title_with_word("Scrum")
filter_out_title_with_word("Staff")
filter_out_title_with_word("Firmware")
filter_out_title_with_word("Infrastructure")
filter_out_title_with_word("defence")

filter_out_company("ClickJobs.io")
filter_out_company("European Recruitment")
filter_out_company("AFRY")
filter_out_company("IC Resources")
filter_out_company("Jaguar Land Rover")
filter_out_company("Atlas Recruitment Group Ltd")
filter_out_company("CareerAddict")
filter_out_company("Agoda")

filter_out_description_without_word('the')

export_to_csv()