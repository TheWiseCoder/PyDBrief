import sys
import logging
from datetime import datetime
import multiprocessing
import pandas as pd
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import oracledb
import psycopg2
import readline # support use of cursors in user input
import getpass
import cx_Oracle
import pydb_oracle as migration_source
import pydb_postgres as migration_target

def convert(errors: list[str], source_engine: str, target_engine: str) -> dict:
    """
    Connects to the source and target databases, then migrates a list of defined schema.
    """

    # create the logfile
    migration_target.create_logfile()

    # get settings for migration
    migration_config = migration_target.get_migration_config()
    source_config = migration_source.get_source_config()
    target_config = migration_target.get_target_config()

    # check the schema exist on the source database
    source_engine = migration_source.connect_to_source(source_config)
    migration_source.check_schema_exist(source_engine,source_config['schema_list'])

    # check and remove null characters in strings
    migration_source.check_for_nulls(source_engine,source_config['schema_list'],remove=True)

    # create a new database on the target
    target_engine = migration_target.connect_to_target(target_config)
    migration_target.drop_connections(target_config['database'],target_engine)
    migration_target.drop_database(target_config['database'],target_engine)
    migration_target.create_database(target_config['database'],target_engine)

    # create the schema on the target database
    target_engine = migration_target.connect_to_target(target_config,target_config['database'])
    migration_target.create_target_schema(source_config['schema_list'],source_engine,target_engine)

    # run the migration
    migration_target.migrate(source_config,target_config,migration_config)
    
    return {}
