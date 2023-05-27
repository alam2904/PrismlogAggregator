from collections import OrderedDict
from datetime import datetime
from decimal import Decimal

class QueryExecutor:
    def __init__(self, connection):
        self.connection = connection
        self.formatted_row = OrderedDict()
        self.is_success = False

    def execute(self, query_type, query):
        if query_type == "SELECT":
            column_names, results = self.connection.execute_select(query)
            # column_names = [desc[0] for desc in self.connection.cursor().description]
            
            for row in results:
                for i, item in enumerate(row):
                    column_name = column_names[i]
                    if isinstance(item, long):
                        self.formatted_row[column_name] = int(item)
                    elif isinstance(item, Decimal):
                        self.formatted_row[column_name] = float(item)
                    elif isinstance(item, datetime):
                        self.formatted_row[column_name] = item.strftime("%Y-%m-%d %H:%M:%S")
                    elif item is None:
                        self.formatted_row[column_name] = ''
                    else:
                        self.formatted_row[column_name] = item
        
        elif query_type == "UPDATE":
            # Prepare the SQL statement
            # query = "UPDATE subscriptions SET QUEUE_ID = 99, PMT_STATUS = '%d', TASK_TYPE = '%s' WHERE SBN_ID = '%d'"
            if self.connection.execute_update(query):
                self.is_success = True