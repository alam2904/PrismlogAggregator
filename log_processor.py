from tlog_parser import TlogParser
from log_parser import TDLogParser
import logging

class PROCESSOR:
    """
    Processor class
    """
    def __init__(self, msisdn, input_date, outputDirectory_object):
        self.msisdn = msisdn
        self.input_date = input_date
        self.outputDirectory_object = outputDirectory_object

    def process(self, is_tomcat, is_prism, is_sms, is_tomcat_tlog_path, is_prism_tlog_path, is_sms_tlog_path, initializedPath_object):
        dictionary_of_tlogs = {}
        tlog_record_list_prism = []
        tlog_record_list_tomcat = []
        tlog_record_list_sms = []
        worker_log_recod_list = []
        dictionary_of_search_value = {"TIMESTAMP" : "","THREAD" : "","MSISDN" : "","SUB_TYPE" : "","CHARGE_TYPE": ""}
        dictionary_of_search_value_sms = {"TIMESTAMP": "","THREAD" : "","MSISDN" : "","SRNO" : "","HANDLER" : "","STATUS" : "","REMARKS": ""}

        tlogParser_object = TlogParser(self.msisdn, self.input_date, dictionary_of_tlogs, tlog_record_list_prism, tlog_record_list_tomcat, tlog_record_list_sms, initializedPath_object)
        
        if is_tomcat and is_tomcat_tlog_path:
            logging.debug('Tomcat tlog path exists.')
            if tlogParser_object.parse_tomcat():
                
                daemonLogParser_object = TDLogParser(self.input_date, tlogParser_object.dictionary_of_tlogs, dictionary_of_search_value, worker_log_recod_list, initializedPath_object, is_tomcat, False, False, self.outputDirectory_object)
                daemonLogParser_object.parse(tlogParser_object, self.msisdn)
            else:
                logging.error('No issue tlog found. Hence not fetching the tomcat log.')
                
        if is_prism and is_prism_tlog_path:
            logging.debug('Prism tlog path exists.')
            if tlogParser_object.parse_prism():
                    
                daemonLogParser_object = TDLogParser(self.input_date, tlogParser_object.dictionary_of_tlogs, dictionary_of_search_value, worker_log_recod_list, initializedPath_object, False, is_prism, False, self.outputDirectory_object)
                daemonLogParser_object.parse(tlogParser_object, self.msisdn)
            
            else:
                logging.error('No issue tlog found. Hence not fetching the prism log.')
        
        if is_sms and is_sms_tlog_path:
            logging.debug('Sms tlog path exists.')
            if tlogParser_object.parse_sms():
                    
                daemonLogParser_object = TDLogParser(self.input_date, tlogParser_object.dictionary_of_tlogs, dictionary_of_search_value_sms, worker_log_recod_list, initializedPath_object, False, False, is_sms, self.outputDirectory_object)
                daemonLogParser_object.parse_sms_td(tlogParser_object, self.msisdn)
            
            else:
                logging.error('No issue tlog found. Hence not fetching the prism log.')
            
        else:
            logging.error('Prism tlog path does not exists. Hence not fetching the logs')