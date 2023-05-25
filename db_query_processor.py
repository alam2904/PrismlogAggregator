import logging


class DB_QUERY_PROCESSOR:
    """
        This is the class responsible for SELECT and UPDATE db query processing
        based on various subscription condition 
    """
    def __init__(self, sbn_thread_dict, tlog_data_dict):
        self.sbn_thread_dict = sbn_thread_dict
        self.tlog_data_dict = tlog_data_dict
        
    def query_formatter(self):
        for sbn_id, thread in self.sbn_thread_dict.items():
            # Prepare the SQL statement
            select_query = "SELECT * FROM SUBSCRIPTIONS WHERE sbn_id='{}'".format(sbn_id)
                    
            logging.info('SELECT_QUERY: %s', select_query)