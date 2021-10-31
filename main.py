from marketbeat import marketbeat
import sqlite3
import logging
from time import sleep
from random import randint
import pandas as pd

# Database name
DB_NAME = "marketbeat2.db"
TABLE_NAME = "ratings"

log_file = 'marketbeat.log'
logging.basicConfig(filename=log_file,
                    filemode='a',
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s: %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p')

if __name__ == '__main__':
    conn = sqlite3.connect(DB_NAME)
    df = marketbeat.getDailyRatingsTable()

    logging.info("Writing {} rows to {}".format(df.shape[0], DB_NAME))
    df.to_sql(TABLE_NAME, conn, index=False, if_exists='append')


