
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

entries = [
    {'id' : 7, 'name' : 'Alice', 'last name' : 'Halison', 'nationality' : 'Canadian'},
    {'id' : 10, 'name' : 'Jack', 'last name' : 'hann', 'nationality' : 'German'},
    {'id' : 9, 'name' : 'James', 'last name' : 'Haltmann', 'nationality' : 'French'}
]
stmt = students.insert().values(entries)

logger.debug(f"Insert statement: {stmt.compile().params}")


# create a connection
with engine.connect() as conn:

    # s = students.select()
    result = conn.execute(stmt)

    conn.commit()
