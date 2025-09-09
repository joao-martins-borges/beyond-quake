import psycopg2

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


class Database:
    def __init__(self, dbname, user, password, host, port):
        self.conn = psycopg2.connect(
            database=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )

    #execute query in database
    def execute(self, query, params=None):
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            self.conn.commit()
            return cur

    #close database cursor
    def close(self):
        if self.conn:
            self.conn.close()

    #retrieve records from database
    def fetch_one(self, query, params=None):
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, params)
                result = cur.fetchone()
                logging.info("fetch_one executed: %s", query)
                return result
        except Exception as e:
            logging.error("Error in fetch_one with query %s and params %s: %s", query, params, e)
            return None

    #retrieve records from database
    def fetch_all(self, query, params=None):
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, params)
                results = cur.fetchall()
                logging.info("fetch_all executed: %s", query)
                return results
        except Exception as e:
            logging.error("Error in fetch_all with query %s and params %s: %s", query, params, e)
            return []

    #Insert new records in the database and update record if it already exists
    def insert_earthquake(self, earthquake):
        insert_query = '''
        INSERT INTO bronze.earthquakes (id, location, magnitude, depth, timestamp, updated_utc)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            location = EXCLUDED.location,
            magnitude = EXCLUDED.magnitude,
            depth = EXCLUDED.depth,
            timestamp = EXCLUDED.timestamp,
            updated_utc = EXCLUDED.updated_utc
        RETURNING id;

        '''
        with self.conn.cursor() as cur:
            cur.execute(insert_query, (earthquake["id"], earthquake["location"], earthquake["magnitude"], earthquake["depth_km"], earthquake["time_utc"], earthquake["updated_utc"]))
            self.conn.commit()
            return cur.fetchone()[0]