from datetime import datetime
from tlog_parser import TlogParser
from log_parser import TDLogParser
from automation import Automation
import logging

class PROCESSOR:
    """
    Processor class
    """
    def __init__(self, msisdn, fmsisdn, input_date, outputDirectory_object, validation_object, random_arg):

        self.msisdn = msisdn
        self.mdn = fmsisdn
        self.input_date = input_date
        self.outputDirectory_object = outputDirectory_object
        # self.file = file
        self.today_date = str(datetime.strftime(datetime.today(), "%Y-%m-%d")).replace("-", "")
        # self.today_date_time = datetime.strftime(datetime.today(), "%Y-%m-%d %H:%M:%S")
        self.validation_object = validation_object
        self.random_arg = random_arg
        self.dict_key = ""
        
        self.outputDirectory_object = outputDirectory_object
        self.prismd_thread_outfile = f"{self.outputDirectory_object}/{self.random_arg}_{self.input_date}_{self.mdn}_{self.today_date}__prismd.log"
        self.tomcat_thread_outfile = f"{self.outputDirectory_object}/{self.random_arg}_{self.input_date}_{self.mdn}_{self.today_date}__tomcat.log"
        self.smsd_thread_outfile = f"{self.outputDirectory_object}/{self.random_arg}_{self.input_date}_{self.mdn}_{self.today_date}__smsd.log"
        self.trimmed_prism_outfile = f"{self.outputDirectory_object}/{self.random_arg}_{self.input_date}_{self.mdn}_{self.today_date}__trimmed_prismd.log"
        self.trimmed_tomcat_outfile = f"{self.outputDirectory_object}/{self.random_arg}_{self.input_date}_{self.mdn}_{self.today_date}__trimmed_tomcat.log"
        self.issue_tlog_path = f"{self.outputDirectory_object}/{self.random_arg}_{self.input_date}_{self.mdn}_{self.today_date}__issue_tlog_record.txt"
                
        
    def process_automation(self, is_tomcat_tlog_path, is_prism_tlog_path, initializedPath_object):
        tlog_record_list_prism = []
        tlog_record_list_tomcat = []
        plog_record_list_tomcat = []
        plog_record_list_prism = []

        # st_date = datetime.strptime(self.validation_object.f_diff_date_time, "%Y%m%d%H%M%S")
        st_date = round(datetime.now().timestamp() * 1000)
        # end_date = datetime.strptime(self.validation_object.f_cur_date_time, "%Y%m%d%H%M%S")
        
        
        tlogParser_object = TlogParser(self.msisdn, self.input_date, None, tlog_record_list_prism, tlog_record_list_tomcat, None, plog_record_list_prism, plog_record_list_tomcat, initializedPath_object)
        # tlogParser_object = TlogParser(self.msisdn, tlog_record_list_prism, tlog_record_list_tomcat, initializedPath_object)
        
        if self.validation_object.keyword == "alog":
            if initializedPath_object.is_access_path:
                data_automation_outfile = f"{self.outputDirectory_object}/{self.random_arg}_{self.msisdn}_{st_date}_{self.today_date}_alog_data.txt"
                auto = Automation()
                
                access_path = initializedPath_object.tomcat_log_path_dict[initializedPath_object.tomcat_access_path]
                auto.parse_alog_btw_timestamps(self.msisdn, self.validation_object, data_automation_outfile, access_path)
            else:
                logging.error('access path does not exists.')
        
        elif self.validation_object.keyword == "tlog":
            data_automation_outfile = f"{self.outputDirectory_object}/{self.random_arg}_{self.msisdn}_{st_date}_{self.today_date}_tlog_data.txt"
            if is_tomcat_tlog_path:
                logging.debug('Tomcat tlog path exists.')
                
                if tlogParser_object.parse_tomcat_automation(self.validation_object, data_automation_outfile, self.validation_object.keyword):
                    pass
                    
            if is_prism_tlog_path:
                logging.debug('Prism tlog path exists.')
                if tlogParser_object.parse_prism_automation(self.validation_object, data_automation_outfile, self.validation_object.keyword):
                    pass
            else:
                logging.error('tlog path does not exists. Hence tlog data could not be fetched.')
        
        elif self.validation_object.keyword == "plog":
            data_automation_outfile = f"{self.outputDirectory_object}/{self.random_arg}_{self.msisdn}_{st_date}_{self.today_date}_plog_data.txt"
            if is_tomcat_tlog_path:
                logging.debug('Tomcat tlog path exists.')
                
                if tlogParser_object.parse_tomcat_automation(self.validation_object, data_automation_outfile, self.validation_object.keyword):
                    pass
                    
            if is_prism_tlog_path:
                logging.debug('Prism tlog path exists.')
                if tlogParser_object.parse_prism_automation(self.validation_object, data_automation_outfile, self.validation_object.keyword):
                    pass
            else:
                logging.error('tlog path does not exists. Hence tlog data could not be fetched.')
            
    def process(self, is_tomcat_tlog_path, is_prism_tlog_path, is_sms_tlog_path, initializedPath_object):
        dictionary_of_tlogs = {}
        tlog_record_list_prism = []
        tlog_record_list_tomcat = []
        tlog_record_list_sms = []
        plog_record_list_tomcat = []
        plog_record_list_prism = []
        worker_log_recod_list = []
        dictionary_tlog_to_search = {}
        
        is_error_tlog = is_lowbal_tlog = is_retry_tlog = is_nhf_tlog = is_await_push_tlog = is_timeout_tlog = is_handler_exp = False
        is_issue_sms_tlog = False
        
        dictionary_of_search_value = {"TIMESTAMP" : "","THREAD" : "","MSISDN" : "","SUB_TYPE" : "","CHARGE_TYPE": ""}
        dictionary_of_search_value_sms = {"TIMESTAMP": "","THREAD" : "","MSISDN" : "","SRNO" : "","HANDLER" : "","STATUS" : "","REMARKS": ""}

        tlogParser_object = TlogParser(self.msisdn, self.input_date, dictionary_of_tlogs, tlog_record_list_prism, tlog_record_list_tomcat, tlog_record_list_sms, plog_record_list_prism, plog_record_list_tomcat, initializedPath_object)
        
        if is_tomcat_tlog_path:
            logging.debug('Tomcat tlog path exists.')
            if tlogParser_object.parse_tomcat(self.validation_object):
                
                logging.info('dictionary of tlogs: %s', tlogParser_object.dictionary_of_tlogs)
                if tlogParser_object.dictionary_of_tlogs:
                    for key, value in tlogParser_object.dictionary_of_tlogs.items():
                        daemonLogParser_object = TDLogParser(self.msisdn, self.input_date, tlogParser_object.dictionary_of_tlogs, dictionary_of_search_value, worker_log_recod_list, initializedPath_object, self.tomcat_thread_outfile, self.prismd_thread_outfile, self.smsd_thread_outfile, self.trimmed_tomcat_outfile, self.trimmed_prism_outfile, self.issue_tlog_path, dictionary_tlog_to_search, self.dict_key)
                        daemonLogParser_object.parse(tlogParser_object, self.msisdn, key, value, is_error_tlog, is_lowbal_tlog, is_retry_tlog, is_nhf_tlog, is_await_push_tlog, is_timeout_tlog, is_handler_exp)
                else:
                    logging.error('No issue tlog found. Hence not fetching the tomcat log.')
            else:
                logging.error('No tlog found. Hence not fetching the tomcat log.')
                
        if is_prism_tlog_path:
            tlogParser_object.dictionary_of_tlogs = {}
            logging.debug('Prism tlog path exists.')
            if tlogParser_object.parse_prism(self.validation_object):   
            
                logging.info('dictionary of tlogs: %s', tlogParser_object.dictionary_of_tlogs)
                if tlogParser_object.dictionary_of_tlogs:
                    for key, value in tlogParser_object.dictionary_of_tlogs.items():
                        daemonLogParser_object = TDLogParser(self.msisdn, self.input_date, tlogParser_object.dictionary_of_tlogs, dictionary_of_search_value, worker_log_recod_list, initializedPath_object, self.tomcat_thread_outfile, self.prismd_thread_outfile, self.smsd_thread_outfile, self.trimmed_tomcat_outfile, self.trimmed_prism_outfile, self.issue_tlog_path, dictionary_tlog_to_search, self.dict_key)
                        logging.info('key: %s and thread : %s', key, tlogParser_object.dictionary_of_tlogs[key])
                        daemonLogParser_object.parse(tlogParser_object, self.msisdn, key, value, is_error_tlog, is_lowbal_tlog, is_retry_tlog, is_nhf_tlog, is_await_push_tlog, is_timeout_tlog, is_handler_exp)
                else:
                    logging.error('No issue tlog found. Hence not fetching the prism log.')
            else:
                logging.error('No tlog found. Hence not fetching the prism log.')
        
        if is_sms_tlog_path:
            tlogParser_object.dictionary_of_tlogs = {}
            logging.debug('Sms tlog path exists.')
            if tlogParser_object.parse_sms(self.validation_object):
                
                logging.info('dictionary of tlogs: %s', tlogParser_object.dictionary_of_tlogs)
                if tlogParser_object.dictionary_of_tlogs:
                    for key, value in tlogParser_object.dictionary_of_tlogs.items():
                        daemonLogParser_object = TDLogParser(self.msisdn, self.input_date, tlogParser_object.dictionary_of_tlogs, dictionary_of_search_value_sms, worker_log_recod_list, initializedPath_object, self.tomcat_thread_outfile, self.prismd_thread_outfile, self.smsd_thread_outfile, self.trimmed_tomcat_outfile, self.trimmed_prism_outfile, self.issue_tlog_path, dictionary_tlog_to_search, self.dict_key)
                        logging.info('key: %s and thread : %s', key, tlogParser_object.dictionary_of_tlogs[key])
                        daemonLogParser_object.parse_sms_td(tlogParser_object, self.msisdn, key, is_issue_sms_tlog)
                else:
                    logging.error('No issue tlog found. Hence not fetching the sms log.')
            else:
                logging.error('No tlog found. Hence not fetching the sms log.')
                
        else:
            logging.error('tlog path does not exists. Hence not fetching the logs.')