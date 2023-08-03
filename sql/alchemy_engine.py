import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, types
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.engine import URL
import pyodbc

load_dotenv()


config = os.environ
# DATA_CONX = config["sql.alchemy"].format(config["sql.data"])
# data_engine = create_engine(DATA_CONX, fast_executemany = True, connect_args={"check_same_thread": False}, echo=True)

# session_factory_data = sessionmaker(bind=data_engine)
# data_session = scoped_session(session_factory_data)

def object_as_dict(obj):
	return {c.key: getattr(obj, c.key)
			for c in inspect(obj).mapper.column_attrs}
_trusted = 'yes' if config["sql.trusted_connection"] else 'no'

def url_assign(database_type):
    url = {
        'import': f"mssql+pyodbc://{config['sql.username']}:{config['sql.password']}@{config['sql.server']}/{config['import.database']}?trusted_connection={_trusted}&driver=SQL+Server+Native+Client+11.0",
        'extract': f"mssql+pyodbc://{config['sql.username']}:{config['sql.password']}@{config['sql.server']}/{config['extract.database']}?trusted_connection={_trusted}&driver=SQL+Server+Native+Client+11.0"
    }
    return url[database_type]

url_import = url_assign('import')
import_engine = create_engine(url_import, connect_args={"check_same_thread": False}, echo=False, fast_executemany=True)
import_session_factory = sessionmaker(bind=import_engine)
import_session = scoped_session(import_session_factory)

url_extract = url_assign('extract')
extract_engine = create_engine(url_extract, connect_args={"check_same_thread": False}, echo=False, fast_executemany=True)
extract_session_factory = sessionmaker(bind=extract_engine)
extract_session = scoped_session(extract_session_factory)

