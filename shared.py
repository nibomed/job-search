import logging
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# Parameters to choose
logging.basicConfig(level=logging.INFO)
#path_to_db = 'test.db'
path_to_db = 'new_horizons.db'
Base = declarative_base()

class BaseJob(Base):
    __tablename__ = 'jobs'
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=func.now())
    session = Column(Integer, nullable=False)
    title = Column(String, nullable=False)
    company = Column(String, nullable=False)
    location = Column(String, nullable=False)
    city = Column(String, nullable=False)
    link = Column(String, nullable=False)
    tags = Column(String, nullable=False)
    duplicationId = Column(Integer, default=0)
    filteredReason = Column(String, default='')
    description = Column(String, nullable=False)
    SesionMaker = None

    def is_valid(self):
        # Check that all non-nullable columns have non-null values
        if not self.title or self.title.strip() == '':
            print("Title is empty")
            return False
        if not self.company or self.company.strip() == '':
            print("Company is empty")
            return False
        if not self.location or self.location.strip() == '':
            print("Location is empty")
            return False
        if not self.city or self.city.strip() == '':
            print("City is empty")
            return False
        if not self.link or self.link.strip() == '':
            print("Link is empty")
            return False
        if not self.tags or self.tags.strip() == '':
            print("Tags are empty")
            return False
        if not self.description or self.description.strip() == '':
            print("Description is empty")
            return False     
        if self.session is None:
            print("Session is empty")
            return False
        # All non-nullable columns have non-null values and are not empty strings
        return True
    
    def log(self):
        print(f"{self.title} ({self.company}) {self.city} ({self.location}) <{len(self.description)}>\n{self.tags}\n{self.link[:75]}...")
    
    @staticmethod
    def get_handler():
        if not BaseJob.SesionMaker:
            engine = create_engine('sqlite:///' + path_to_db)
            Base.metadata.create_all(engine)
            BaseJob.SesionMaker = sessionmaker(bind=engine)
        return BaseJob.SesionMaker()

    @staticmethod  
    def get_last_session():
        session = BaseJob.get_handler()
        result = session.query(func.coalesce(func.max(BaseJob.session), 0)).scalar()
        session.close()
        return result