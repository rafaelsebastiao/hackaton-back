from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from hackaton.settings import Settings

engine = create_engine(Settings().DATABASE_URL)


def get_session():
    yield Session(engine)
