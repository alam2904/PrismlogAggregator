from tlog_parser import TlogParser
from daemonlog_parser import PrismDaemonLogParser
import logging

class PROCESSOR:
    """
    Processor class
    """
    def __init__(self, msisdn, input_date):
        self.msisdn = msisdn
        self.input_date = input_date

    def process(self, is_tomcat, is_prism, is_tomcat_tlog_path, is_prism_tlog_path, initializedPath_object):
        dictionary_of_tlogs = {}
        tlog_record_list = []
        worker_log_recod_list = []
        dictionary_of_search_value = {"TIMESTAMP" : "","THREAD" : "","MSISDN" : "","SUB_TYPE" : ""}

        tlogParser_object = TlogParser(self.msisdn, self.input_date, dictionary_of_tlogs, tlog_record_list, initializedPath_object)
        
        if is_tomcat and is_tomcat_tlog_path:
            pass
        if is_prism and is_prism_tlog_path:
            logging.debug('Prism tlog path exists.')
            if tlogParser_object.parse_prism():
                logging.debug('Prism tlog parsed successfully')
                daemonLogParser_object = PrismDaemonLogParser(self.input_date, tlogParser_object.dictionary_of_tlogs, dictionary_of_search_value, worker_log_recod_list, initializedPath_object)
                daemonLogParser_object.parse()
            else:
                logging.error('No issue tlog found. Hence not fetching the prism log.')
        else:
            logging.error('Prism tlog path does not exists. Hence not fetching the logs')