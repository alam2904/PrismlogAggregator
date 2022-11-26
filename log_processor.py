from datetime import datetime
from tlog_parser import TlogParser
from log_parser import TDLogParser
import logging

class PROCESSOR:
    """
    Processor class
    """
    def __init__(self, msisdn, input_date, outputDirectory_object, file):

        self.msisdn = msisdn
        self.input_date = input_date
        self.outputDirectory_object = outputDirectory_object
        self.file = file
        self.today_date_time = datetime.strftime(datetime.today(), "%Y-%m-%d")

        self.outputDirectory_object = outputDirectory_object
        self.prismd_thread_outfile = f"{self.outputDirectory_object}/{self.input_date}_{self.msisdn}_{self.today_date_time}_prismd.log"
        self.tomcat_thread_outfile = f"{self.outputDirectory_object}/{self.input_date}_{self.msisdn}_{self.today_date_time}_tomcat.log"
        self.smsd_thread_outfile = f"{self.outputDirectory_object}/{self.input_date}_{self.msisdn}_{self.today_date_time}_smsd.log"
        self.trimmed_prism_outfile = f"{self.outputDirectory_object}/{self.input_date}_{self.msisdn}_{self.today_date_time}_trimmed_prismd.log"
        self.trimmed_tomcat_outfile = f"{self.outputDirectory_object}/{self.input_date}_{self.msisdn}_{self.today_date_time}_trimmed_tomcat.log"
        self.issue_tlog_path = f"{self.outputDirectory_object}/{self.input_date}_{self.msisdn}_{self.today_date_time}_issue_tlog_record.txt"

    def process(self, is_tomcat_tlog_path, is_prism_tlog_path, is_sms_tlog_path, initializedPath_object):
        dictionary_of_tlogs = {}
        tlog_record_list_prism = []
        tlog_record_list_tomcat = []
        tlog_record_list_sms = []
        worker_log_recod_list = []
        dictionary_of_search_value = {"TIMESTAMP" : "","THREAD" : "","MSISDN" : "","SUB_TYPE" : "","CHARGE_TYPE": ""}
        dictionary_of_search_value_sms = {"TIMESTAMP": "","THREAD" : "","MSISDN" : "","SRNO" : "","HANDLER" : "","STATUS" : "","REMARKS": ""}

        tlogParser_object = TlogParser(self.msisdn, self.input_date, dictionary_of_tlogs, tlog_record_list_prism, tlog_record_list_tomcat, tlog_record_list_sms, initializedPath_object)
        
        if is_tomcat_tlog_path:
            logging.debug('Tomcat tlog path exists.')
            if tlogParser_object.parse_tomcat():
                
                daemonLogParser_object = TDLogParser(self.msisdn, self.input_date, tlogParser_object.dictionary_of_tlogs, dictionary_of_search_value, worker_log_recod_list, initializedPath_object, self.tomcat_thread_outfile, self.prismd_thread_outfile, self.smsd_thread_outfile, self.trimmed_tomcat_outfile, self.trimmed_prism_outfile, self.issue_tlog_path, self.file)
                daemonLogParser_object.parse(tlogParser_object, self.msisdn)
            else:
                logging.error('No issue tlog found. Hence not fetching the tomcat log.')
                
        if is_prism_tlog_path:
            logging.debug('Prism tlog path exists.')
            if tlogParser_object.parse_prism():
                    
                daemonLogParser_object = TDLogParser(self.msisdn, self.input_date, tlogParser_object.dictionary_of_tlogs, dictionary_of_search_value, worker_log_recod_list, initializedPath_object, self.tomcat_thread_outfile, self.prismd_thread_outfile, self.smsd_thread_outfile, self.trimmed_tomcat_outfile, self.trimmed_prism_outfile, self.issue_tlog_path, self.file)
                daemonLogParser_object.parse(tlogParser_object, self.msisdn)
            
            else:
                logging.error('No issue tlog found. Hence not fetching the prism log.')
        
        if is_sms_tlog_path:
            logging.debug('Sms tlog path exists.')
            if tlogParser_object.parse_sms():
                    
                daemonLogParser_object = TDLogParser(self.msisdn, self.input_date, tlogParser_object.dictionary_of_tlogs, dictionary_of_search_value_sms, worker_log_recod_list, initializedPath_object, self.tomcat_thread_outfile, self.prismd_thread_outfile, self.smsd_thread_outfile, self.trimmed_tomcat_outfile, self.trimmed_prism_outfile, self.issue_tlog_path, self.file)
                daemonLogParser_object.parse_sms_td(tlogParser_object, self.msisdn)
            
            else:
                logging.error('No issue tlog found. Hence not fetching the sms log.')
            
        else:
            logging.error('tlog path does not exists. Hence not fetching the logs')