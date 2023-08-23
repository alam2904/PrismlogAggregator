from collections import OrderedDict
import logging
import json
from update_query_criteria import UpdateQueryCriteria
from prism_utils import get_db_parameters, query_executor


class SubscriptionEventController:
    """
        This is the class responsible for SELECT and UPDATE db query processing
        based on various subscription condition 
    """
    def __init__(self, config, pname, validation_object, sbn_thread_dict, process_subs_data):
        self.sbn_thread_dict = sbn_thread_dict
        self.validation_object = validation_object
        self.process_subs_data = process_subs_data
        self.transaction_data = []
        self.config = config
        self.pname = pname
        self.db_name, self.db_host = get_db_parameters(config)
        
    def get_subscription_event(self, transaction_table, is_reprocessing_required, reprocess_sbnId=None):       
        try:
            if self.process_subs_data:
                for sbn_event_Id, thread in self.sbn_thread_dict.items():
                    # Prepare the SQL statement
                    if transaction_table == "SUBSCRIPTIONS":
                        query = """
                                    SELECT * FROM SUBSCRIPTIONS
                                    WHERE SBN_ID = %s
                                """ % (sbn_event_Id)
                    
                    elif transaction_table == "EVENTS":
                        self.constructor_parameter_reinitialize()
                        query = """
                                    SELECT * FROM EVENTS
                                    WHERE EVENT_ID = %s
                                """ % (sbn_event_Id)
                        
                    query_type = "SELECT"
                    # logging.info('SELECT_QUERY: %s', query)
                    
                    transaction_object = query_executor(self.db_name, self.db_host, query, query_type)
                    
                    if transaction_object:
                        logging.info("subscription json object: %s", transaction_object)
                        
                        self.transaction_data.append(transaction_object)
                        # self.transaction_data.append(json.loads(transaction_object, object_pairs_hook=OrderedDict))
                        
                        if is_reprocessing_required and self.transaction_data:
                            transaction_record = self.get_subscription_event_dict(transaction_table)
                            if transaction_record:
                                self.execute_update(sbn_event_Id, transaction_record)
                           
            else:
                query_type = "SELECT"
                query = "SELECT * FROM SUBSCRIPTIONS WHERE SBN_ID = %s" % (reprocess_sbnId)
                
                subscription_object = query_executor(self.db_name, self.db_host, query, query_type)
                
                if subscription_object:
                    logging.info("subscription json object after update: %s", subscription_object)
                    self.constructor_parameter_reinitialize()
                    self.transaction_data.append(subscription_object)
                    # self.transaction_data.append(json.loads(subscription_object, object_pairs_hook=OrderedDict))
            
            return self.transaction_data
        except Exception as ex:
            logging.exception(ex)
    
    def get_subscription_event_dict(self):
        for subscriptionEventRecords in self.transaction_data:
            logging.info('subscription_data: %s', subscriptionEventRecords)
            if subscriptionEventRecords:
                for subscriptionEventRecord in subscriptionEventRecords:
                    logging.info('subscription record: %s', subscriptionEventRecord)
                    if subscriptionEventRecord:
                        return subscriptionEventRecord
        
    def execute_update(self, sbn_Id, transaction_record):
        query_type = "UPDATE"
        result = False
        updateCriteria_object = UpdateQueryCriteria(self.config, self.validation_object, transaction_record, sbn_Id)
        logging.info("SUBSCRIPTIONS_SUB_STATUS: %s", transaction_record["sub_status"])
        
        try:
            if (transaction_record["sub_status"] not in ('E', 'F') and transaction_record["task_type"] != 'N'
                and transaction_record["task_status"] in (97, 98)):
                
                updateCriteria_object.update_query_formatter()
        except KeyError as err:
            logging.debug(err)
            
        if updateCriteria_object.update_query:
            query = updateCriteria_object.update_query
                
            logging.info("UPDATE QUERY: %s", query)
            result = query_executor(self.db_name, self.db_host, query, query_type)
            
            if result:
                logging.info('is update success: %s', result)
                self.process_subs_data = False
                self.get_subscription_event("SUBSCRIPTIONS", False, sbn_Id)
            else:
                self.transaction_data = None
        else:
            self.transaction_data = None
            
    
    def constructor_parameter_reinitialize(self):
        self.transaction_data = []
            
            
            