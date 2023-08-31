# database_manager.py
import mysql.connector

class DatabaseManager:
    def __init__(self, host, user, password, database):
        # Initialize a connection to the MySQL database
        self.connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        # Create a cursor object to interact with the database
        self.cursor = self.connection.cursor()

    def execute_query(self, query):
        # Execute the provided SQL query using the cursor
        self.cursor.execute(query)
        # Fetch the results of the query
        result = self.cursor.fetchall()
        return result

    def close(self):
        # Close the cursor and the database connection
        self.cursor.close()
        self.connection.close()