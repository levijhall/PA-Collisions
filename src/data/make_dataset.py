# -*- coding: utf-8 -*-
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv

import pandas as pd
from zipfile import ZipFile
from sqlite3 import connect

import schema

DB_NAME = 'data/raw/collisions.db'


def connect_to_db():
    return connect(DB_NAME)


def delete_db(conn):
    'Drop all tables'

    logger = logging.getLogger(__name__)
    logger.info('Deleting the database.')

    curs = conn.cursor()

    for table in schema.Schema:
        command = 'DROP TABLE IF EXISTS "{}"'.format(table)
        # print(command)
        curs.execute(command)

    conn.commit()


def create_db(conn):
    'Creates the schema for the database'
    curs = conn.cursor()

    logger = logging.getLogger(__name__)
    logger.info('Creating the database.')

    # Generate the tables from the dictionaries
    for table in schema.Schema:
        start = 'CREATE TABLE IF NOT EXISTS "{}" ('.format(table)

        end = ');'
        fields = []
        for field in schema.Schema[table]:
            line = '"{}" {},'.format(field, schema.Schema[table][field])
            fields.append(line)

        if table == "CRASH":
            primary = ', PRIMARY KEY("CRN")'
        else:
            primary = ''

        command = ''.join([start, ''.join(fields).strip(','), primary, end])
        # print(command)
        curs.execute(command)
    conn.commit()


def compile_db(conn):
    'Load all data into dataset from zip files'
    for file in Path('data/raw').iterdir():

        if not file.suffix == '.zip':
            continue

        with ZipFile(file) as zp:
            for sheet in zp.namelist():
                if not sheet.endswith('.csv'):
                    continue

                logger = logging.getLogger(__name__)
                logger.info('Appending {} to database.'.format(sheet))

                table = sheet.split("_")[0]
                dtypes = dict(map(lambda kv: (kv[0], schema.dtpyes[kv[1]]),
                                  schema.Schema[table].items()))

                df = pd.read_csv(zp.open(sheet), dtype=dtypes,
                                 encoding_errors='backslashreplace')

                # Record conflict found with the first entry of 2021 Statewide
                # CRN = 2020085055, exists both in 2021 and 2020
                # Max value of the CRN for 2021 was 2022045804,
                # so 2020085055 will be substituted for 2022045805 in 2021
                if sheet.endswith('2021_Statewide.csv'):
                    df['CRN'].replace(to_replace=2020085055,
                                      value=2022045805,
                                      inplace=True)

                df.to_sql(table, conn, if_exists='append', index=False,
                          dtype=schema.Schema[table])


def main():
    ''' Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    '''
    logger = logging.getLogger(__name__)
    logger.info('making final data set from raw data')

    conn = connect_to_db()
    delete_db(conn)
    create_db(conn)
    compile_db(conn)

    conn.commit()
    conn.close()


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
