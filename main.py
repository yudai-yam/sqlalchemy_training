
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

engine = create_engine('sqlite:///test.db', echo=True)

meta = MetaData()

# create a table
meta.create_all(engine)

students = Table(
    'students', meta,
    Column('id', Integer, primary_key=True),
    Column('name', String),
    Column('last name', String),
    Column('nationality', String)
) 

stmt = students.insert().values(id = 1, name = 'Yudai')

logger.debug(f"Insert statement: {stmt.compile().params}")


# create a connection
with engine.connect() as conn:

    result = conn.execute(stmt)
    conn.commit()
