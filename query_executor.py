from collections import OrderedDict
from datetime import datetime
from decimal import Decimal
import logging

class QueryExecutor:
    def __init__(self, connection):
        self.connection = connection
        self.result_set = []
        self.is_success = False

    def execute(self, query_type, query, params):
        if query_type == "SELECT":
            column_names, results = self.connection.execute_select(query, params)
                
            # column_names = [desc[0] for desc in self.connection.cursor().description]
            logging.info('result set: %s', results)
            
            for row in results:
                formatted_row = OrderedDict()
                for i, item in enumerate(row):
                    column_name = column_names[i]
                    if isinstance(item, long):
                        formatted_row[column_name] = int(item)
                    elif isinstance(item, Decimal):
                        formatted_row[column_name] = float(item)
                    elif isinstance(item, datetime):
                        formatted_row[column_name] = item.strftime("%Y-%m-%d %H:%M:%S")
                    elif item == '':
                        formatted_row[column_name] = ''
                    elif item is None:
                        formatted_row[column_name] = 'NULL'
                    else:
                        formatted_row[column_name] = item
                self.result_set.append(formatted_row)
                    
        elif query_type == "UPDATE":
            # Prepare the SQL statement
            # query = "UPDATE subscriptions SET QUEUE_ID = 99, PMT_STATUS = '%d', TASK_TYPE = '%s' WHERE SBN_ID = '%d'"
            if self.connection.execute_update(query, params):
                self.is_success = True