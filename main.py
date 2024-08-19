
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, text, select, ForeignKey, event, values, update, join
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


def update_multiple(parent_table, child_table):
    
    stmt = parent_table.update().\
    values({parent_table.c.name:'changed_name'}).\
    where(parent_table.c.id == child_table.c.id)

    return stmt

def join_tables(table_1, table_2):
    j = join(table_2, table_1, isouter=True)

    stmt = select(table_1).select_from(j)

    return stmt



def delete_multiple(parent_table, child_table, conn):

    ids_to_be_deleted = []

    for row in conn.execute(select(child_table.c.st_id)):

        logger.debug(f'in a for loop: {row}')
        if row not in conn.execute(select(parent_table.c.id)).fetchall():
            logger.debug(f"the ids from the original student table is: {conn.execute(select(parent_table.c.id)).fetchall()}")
            ids_to_be_deleted.append(row[0])
        logger.debug(f'the id list to be deleted is: {ids_to_be_deleted}')
    

    stmt = child_table.delete().where(child_table.c.st_id.in_(ids_to_be_deleted))

    return stmt

# create a connection
with engine.connect() as conn:
    # stmt = delete_multiple(students, addresses, conn)
    # stmt = update_multiple(students, addresses)
    stmt = join_tables(students, addresses)
    result = conn.execute(stmt)

    conn.commit()

logger.debug(f'the result of the execution is {result}')

# print(result)

for row in result:
    print(row)