import MySQLdb

class DatabaseConnection:
    def __init__(self, host, user, passwd, db):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db = db

    def create_connection(self):
        self.connection = MySQLdb.connect(
            host=self.host,
            user=self.user,
            passwd=self.passwd,
            db=self.db
        )

    def close(self):
        if self.connection:
            self.connection.close()
    
    def execute_select(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
            
        column_names = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()
        cursor.close()
        return column_names, results
    
    def execute_update(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)

        # Check if the query was successful
        rows_affected = cursor.rowcount
        if rows_affected > 0:
            print("Update query executed successfully. Rows affected:%d" %cursor.rowcount)
            # Commit the changes to the database
            self.connection.commit()
            cursor.close()
            return True
        else:
            print("Update query did not affect any rows.")
            cursor.close()
            return False