import psycopg2
class Database:
    def __init__(self, dbname, user, password, host, port):
        self.conn = psycopg2.connect(
            database=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )

    def execute(self, query, params=None):
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            self.conn.commit()
            return cur

    def close(self):
        if self.conn:
            self.conn.close()
    
    def fetch_one(self, query, params=None):
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            result = cur.fetchone()
        return result

    def fetch_all(self, query, params=None):
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            results = cur.fetchall()
        return results

    def insert_earthquake(self, earthquake):
        insert_query = '''
        INSERT INTO bronze.earthquakes (location, magnitude, depth, timestamp)
        VALUES (%s, %s, %s, %s)
        RETURNING id;
        '''
        with self.conn.cursor() as cur:
            cur.execute(insert_query, (earthquake["location"], earthquake["magnitude"], earthquake["depth_km"], earthquake["time_utc"]))
            self.conn.commit()
            return cur.fetchone()[0]