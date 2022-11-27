from datetime import datetime
from tlog_parser import TlogParser
from log_parser import TDLogParser
import logging

class PROCESSOR:
    """
    Processor class
    """
    def __init__(self, msisdn, fmsisdn, input_date, outputDirectory_object, file, validation_object):

        self.msisdn = msisdn
        self.mdn = fmsisdn
        self.input_date = input_date
        self.outputDirectory_object = outputDirectory_object
        self.file = file
        self.today_date = datetime.strftime(datetime.today(), "%Y-%m-%d")
        # self.today_date_time = datetime.strftime(datetime.today(), "%Y-%m-%d %H:%M:%S")
        self.validation_object = validation_object

        self.outputDirectory_object = outputDirectory_object
        self.prismd_thread_outfile = f"{self.outputDirectory_object}/{self.input_date}_{self.mdn}_{self.today_date}_prismd.log"
        self.tomcat_thread_outfile = f"{self.outputDirectory_object}/{self.input_date}_{self.mdn}_{self.today_date}_tomcat.log"
        self.smsd_thread_outfile = f"{self.outputDirectory_object}/{self.input_date}_{self.mdn}_{self.today_date}_smsd.log"
        self.trimmed_prism_outfile = f"{self.outputDirectory_object}/{self.input_date}_{self.mdn}_{self.today_date}_trimmed_prismd.log"
        self.trimmed_tomcat_outfile = f"{self.outputDirectory_object}/{self.input_date}_{self.mdn}_{self.today_date}_trimmed_tomcat.log"
        self.issue_tlog_path = f"{self.outputDirectory_object}/{self.input_date}_{self.mdn}_{self.today_date}_issue_tlog_record.txt"
        self.tlog_data_automation_outfile = f"{self.outputDirectory_object}/{self.validation_object.f_cur_date_time}_{self.validation_object.f_diff_date_time}_{self.today_date}_tlog_data.txt"
    
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
            if tlogParser_object.parse_tomcat(self.validation_object, self.tlog_data_automation_outfile):
                if not self.validation_object.is_tlog:
                    daemonLogParser_object = TDLogParser(self.msisdn, self.input_date, tlogParser_object.dictionary_of_tlogs, dictionary_of_search_value, worker_log_recod_list, initializedPath_object, self.tomcat_thread_outfile, self.prismd_thread_outfile, self.smsd_thread_outfile, self.trimmed_tomcat_outfile, self.trimmed_prism_outfile, self.issue_tlog_path, self.file)
                    daemonLogParser_object.parse(tlogParser_object, self.msisdn)
            else:
                logging.error('No issue tlog found. Hence not fetching the tomcat log.')
                
        if is_prism_tlog_path:
            logging.debug('Prism tlog path exists.')
            if tlogParser_object.parse_prism(self.validation_object, self.tlog_data_automation_outfile):
                 
                if not self.validation_object.is_tlog:   
                    daemonLogParser_object = TDLogParser(self.msisdn, self.input_date, tlogParser_object.dictionary_of_tlogs, dictionary_of_search_value, worker_log_recod_list, initializedPath_object, self.tomcat_thread_outfile, self.prismd_thread_outfile, self.smsd_thread_outfile, self.trimmed_tomcat_outfile, self.trimmed_prism_outfile, self.issue_tlog_path, self.file)
                    daemonLogParser_object.parse(tlogParser_object, self.msisdn)
            
            else:
                logging.error('No issue tlog found. Hence not fetching the prism log.')
        
        if is_sms_tlog_path:
            logging.debug('Sms tlog path exists.')
            if tlogParser_object.parse_sms(self.validation_object, self.tlog_data_automation_outfile):
                if not self.validation_object.is_tlog:    
                    daemonLogParser_object = TDLogParser(self.msisdn, self.input_date, tlogParser_object.dictionary_of_tlogs, dictionary_of_search_value_sms, worker_log_recod_list, initializedPath_object, self.tomcat_thread_outfile, self.prismd_thread_outfile, self.smsd_thread_outfile, self.trimmed_tomcat_outfile, self.trimmed_prism_outfile, self.issue_tlog_path, self.file)
                    daemonLogParser_object.parse_sms_td(tlogParser_object, self.msisdn)
            
            else:
                logging.error('No issue tlog found. Hence not fetching the sms log.')
            
        else:
            logging.error('tlog path does not exists. Hence not fetching the logs')