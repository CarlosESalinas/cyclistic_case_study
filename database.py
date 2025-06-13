import psycopg2
from psycopg2 import OperationalError
from config import DATABASE

class Database:
    def __init__(self):
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                dbname=DATABASE['dbname'],
                user=DATABASE['user'],
                password=DATABASE['password'],
                host=DATABASE['host'],
                port=DATABASE['port']
            )
            self.cursor = self.connection.cursor()
            print("The connection to the database is established successfully.")
        except OperationalError as e:
            print(f"Error to established connection: {e}")

    def disconnect(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()
            print("Connection to the database closed successfully.")

    def is_connected(self):
        """
        Verify if the database connection is active.
        Returns True if the connection is active, False otherwise.
        
        """
        if self.connection is None or self.connection.closed:
            return False

        try:
            # Ejecuta una consulta simple para verificar la conexión
            self.cursor.execute("SELECT 1")
            return True
        except OperationalError:
            return False

    def fetch_all(self, query):
        """
        Execute a SQL query and return all results.
        :param query: SQL query to execute.
        :return: List of tuples with the results of the query.
        """
        try:
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            return results
        except OperationalError as e:
            print(f"Error to execute the query: {e}")
            return None