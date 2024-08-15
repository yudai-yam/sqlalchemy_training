
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, text, select, ForeignKey, event
import logging
from sqlalchemy.sql import alias

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

engine = create_engine('sqlite:///test.db', echo=True)

meta = MetaData()

def _fk_pragma_on_connect(dbapi_con, con_record):
    dbapi_con.execute('pragma foreign_keys=ON')

#event.listen(engine, 'connect', _fk_pragma_on_connect)


students = Table(
    'students', meta,
    Column('id', Integer, primary_key=True),
    Column('name', String),
    Column('last name', String),
    Column('nationality', String)
) 


addresses = Table(
    'address', meta,
    Column('id', Integer, primary_key=True),
    Column('st_id', Integer, ForeignKey('students.id')),
    Column('postal_code', String),
    Column('email', String)
)

# create a table
meta.create_all(engine)

entries = [
    {'id' : 7, 'name' : 'Alice', 'last name' : 'Halison', 'nationality' : 'Canadian'},
    {'id' : 10, 'name' : 'Jack', 'last name' : 'hann', 'nationality' : 'German'},
    {'id' : 9, 'name' : 'James', 'last name' : 'Haltmann', 'nationality' : 'French'}
]

st = students.alias("a")

entries_address = [
   {'st_id':100, 'postal_code':'Cannought Place new Delhi', 'email':'admin@khanna.com'},
]
sql_text = text('SELECT * FROM students WHERE id == 7')

ids_to_be_deleted = []





# create a connection
with engine.connect() as conn:

    for row in conn.execute(select(addresses.c.st_id)):

        logger.debug(f'in a for loop: {row}')
        if row not in conn.execute(select(students.c.id)).fetchall():
            logger.debug(f"the ids from the original student table is: {conn.execute(select(students.c.id)).fetchall()}")
            ids_to_be_deleted.append(row[0])
        logger.debug(f'the id list to be deleted is: {ids_to_be_deleted}')
    

    stmt = addresses.delete().where(addresses.c.st_id.in_(ids_to_be_deleted))
    result = conn.execute(stmt)

    conn.commit()

# print(result)

# for row in result:
#     print(row)