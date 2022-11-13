"""
importing required modules
"""
import logging
import subprocess
from subprocess import PIPE
from datetime import datetime
from pathlib import Path
import re
from daemon_log import DaemonLog
from outfile_writer import FileWriter
from tlog_tag import TaskType, TlogAwaitPushTag, TlogAwaitPushTimeOutTag, TlogErrorTag, TlogHandlerExp, TlogLowBalTag, TlogRetryTag, TlogNHFTag

class TDLogParser:
    """
    Parse the daemon log based on tlog input
    """
    def __init__(self, input_date, dictionary_of_tlogs, dictionary_of_search_value, worker_log_recod_list, initializedPath_object, is_tomcat, is_prism, outputDirectory_object):
        self.input_date = input_date
        self.initializedPath_object = initializedPath_object
        self.dictionary_of_tlogs = dictionary_of_tlogs
        self.dictionary_of_search_value = dictionary_of_search_value
        self.worker_log_recod_list = worker_log_recod_list
        self.is_tomcat = is_tomcat
        self.is_prism = is_prism
        self.__initial_index = 0
        self.__final_index = 0
        self.__task_type = ""
        # self.is_prism_processing_required = True
        self.outputDirectory_object = outputDirectory_object
        self.trimmed_prism_outfile = self.outputDirectory_object/"trimmed_prismd.log"
        self.trimmed_tomcat_outfile = self.outputDirectory_object/"trimmed_tomcat.log"
        self.issue_tlog_path = self.outputDirectory_object/"issue_tlog_record.txt"
        self.issue_tlog_data_prism = ""
        self.issue_tlog_data_tomcat = ""
        self.is_error_tlog = False
        self.is_lowbal_tlog = False
        self.is_retry_tlog = False
        self.is_nhf_tlog = False
        self.is_await_push_tlog = False
        self.is_timeout_tlog = False
        self.is_handler_exp = False
        self.task = ""
        self.acc_log = []
        self.new_line = '\n'
        self.is_issue_in_thread = False

    def parse(self, tlogParser_object, msisdn):
        """
        Parse dictionary of tlogs to get the search value.
        """
        logging.info('Going to parse tlog for ERROR/RETRY/LOWBAL/HNF/AWAIT_PUSH/AWAIT_PUSH_TIMEOUT cases.')
        
        log_writer = FileWriter()
        
        dts = datetime.strptime(self.input_date, "%Y%m%d")
        dtf = dts.strftime("%Y-%m-%d")
        date_formated = dtf.split("-")
        
        self.is_error_tlog = self.parse_tlog(TlogErrorTag, self.is_error_tlog)
        
        if not self.is_error_tlog:
            self.is_lowbal_tlog = self.parse_tlog(TlogLowBalTag, self.is_lowbal_tlog)
        
        if not self.is_lowbal_tlog:
            self.is_retry_tlog = self.parse_tlog(TlogRetryTag, self.is_retry_tlog)
        
        if not self.is_retry_tlog:
            self.is_nhf_tlog = self.parse_tlog(TlogNHFTag, self.is_nhf_tlog)
        
        if not self.is_nhf_tlog:
            self.is_await_push_tlog = self.parse_tlog(TlogAwaitPushTag, self.is_await_push_tlog)
        
        if not self.is_await_push_tlog:
            self.is_timeout_tlog = self.parse_tlog(TlogAwaitPushTimeOutTag, self.is_timeout_tlog)
        
        if not self.is_timeout_tlog:
            self.is_handler_exp = self.parse_tlog(TlogHandlerExp, self.is_handler_exp)
        
        # if tlogParser_object.filtered_prism_tlog and self.is_prism_processing_required and (self.is_error_tlog or self.is_lowbal_tlog or self.is_retry_tlog or self.is_nhf_tlog or self.is_await_push_tlog or self.is_timeout_tlog or self.is_handler_exp):
        if tlogParser_object.filtered_prism_tlog and (self.is_error_tlog or self.is_lowbal_tlog or self.is_retry_tlog or self.is_nhf_tlog or self.is_await_push_tlog or self.is_timeout_tlog or self.is_handler_exp):
                
            access_path = self.initializedPath_object.tomcat_log_path_dict[self.initializedPath_object.tomcat_access_path]
            logging.info('Issue tlog found. Going to fetch access log.')
                
            if self.dictionary_of_tlogs["CHARGE_TYPE"] == 'A':
                self.fetch_access_log(msisdn, "/subscription/ActivateSubscription?", access_path, date_formated)
            elif self.dictionary_of_tlogs["CHARGE_TYPE"] == 'D':
                self.fetch_access_log(msisdn, "/subscription/DeactivateSubscription?", access_path, date_formated)
            elif self.dictionary_of_tlogs["CHARGE_TYPE"] == 'E':
                self.fetch_access_log(msisdn, "/subscription/EventCharge?", access_path, date_formated)
            elif self.dictionary_of_tlogs["CHARGE_TYPE"] == 'U':
                self.fetch_access_log(msisdn, "/subscription/UpgradeSubscription?", access_path, date_formated)
            elif self.dictionary_of_tlogs["CHARGE_TYPE"] == 'T':
                self.fetch_access_log(msisdn, "/subscription/TriggerCharge?", access_path, date_formated)
            elif self.dictionary_of_tlogs["CHARGE_TYPE"] == 'G':
                self.fetch_access_log(msisdn, "/subscription/ChargeGift?", access_path, date_formated)
            elif self.dictionary_of_tlogs["CHARGE_TYPE"] == 'V':
                self.fetch_access_log(msisdn, "/subscription/AddRenewalTrigger?", access_path, date_formated)
    
            log_writer.write_access_log(self.issue_tlog_path, self.acc_log)
            
            self.issue_tlog_data_prism = tlogParser_object.filtered_prism_tlog[-1]
            log_writer.write_issue_tlog(self.issue_tlog_path, self.issue_tlog_data_prism)
            
        
        elif tlogParser_object.filtered_tomcat_tlog and (self.is_error_tlog or self.is_lowbal_tlog or self.is_retry_tlog or self.is_nhf_tlog or self.is_await_push_tlog or self.is_handler_exp):
            
            access_path = self.initializedPath_object.tomcat_log_path_dict[self.initializedPath_object.tomcat_access_path]
            logging.info('Issue tlog found. Going to fetch access log.')
            if self.dictionary_of_tlogs["CHARGE_TYPE"] == 'A':
                self.fetch_access_log(msisdn, "/subscription/RealTimeActivate?", access_path, date_formated)
            elif self.dictionary_of_tlogs["CHARGE_TYPE"] == 'D':
                self.fetch_access_log(msisdn, "/subscription/RealTimeDeactivate?", access_path, date_formated)
            elif self.dictionary_of_tlogs["CHARGE_TYPE"] == 'E':
                self.fetch_access_log(msisdn, "/subscription/RealTimeCharge?", access_path, date_formated)
            elif self.dictionary_of_tlogs["CHARGE_TYPE"] == 'F':
                self.fetch_access_log(msisdn, "/subscription/RealTimeTransactionRefund?", access_path, date_formated)
                
            log_writer.write_access_log(self.issue_tlog_path, self.acc_log)
               
            self.issue_tlog_data_tomcat = tlogParser_object.filtered_tomcat_tlog[-1]
            log_writer.write_issue_tlog(self.issue_tlog_path, self.issue_tlog_data_tomcat)
                
        else:
            logging.debug('No issue tlog found for given msisdn: %s', msisdn)
            logging.debug('Hence not fetching the daemon log.')
            
        self.get_trimmed_thread_log()
    
    def fetch_access_log(self, msisdn, search_string, access_path, date_formated):
        try:
            access_log = subprocess.run(["grep", f"{search_string}", f"{access_path}/localhost_access_log.{date_formated[0]}-{date_formated[1]}-{date_formated[2]}.txt"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
            for data in access_log.stdout.splitlines():
                if re.search(r"\b{}\b".format(str(msisdn)),data):
                    self.acc_log = f"{data}{self.new_line}"
        except subprocess.CalledProcessError as ex:
            try: 
                access_log = subprocess.run(["grep", f"{search_string}", f"{self.initializedPath_object.dict_of_process_dir['tomcat']['PROCESS_HOME_DIR']}/{access_path}/localhost_access_log.{date_formated[0]}-{date_formated[1]}-{date_formated[2]}.txt"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
                for data in access_log.stdout.splitlines():
                    if re.search(r"\b{}\b".format(str(msisdn)),data):
                        self.acc_log = f"{data}{self.new_line}"
            except subprocess.CalledProcessError as ex:
                    logging.info('No access log found')
        

    def parse_tlog(self, tlogTags, is_tlog):
        for key, value in self.dictionary_of_tlogs.items():
            for status in tlogTags:
                if re.search(r"\b{}\b".format(str(status.value)), value):
                    for search_key, search_value in self.dictionary_of_search_value.items():
                        self.dictionary_of_search_value[search_key] = self.dictionary_of_tlogs[search_key]
                        is_tlog = True
                    break
        return is_tlog
            
    def get_trimmed_thread_log(self):
        """
        Get daemon log for the given thread
        """

        # task = ""
        # if len(self.issue_tlog_data_tomcat) != 0 and self.is_prism_processing_required == False:
        log_writer = FileWriter()
        
        if len(self.issue_tlog_data_tomcat) != 0:
            
            if not self.is_await_push_tlog:
                logging.info('Going to fetch tomcat daemon log for the issue thread : %s', self.dictionary_of_search_value["THREAD"])
                daemonLog_object = DaemonLog(self.input_date, self.worker_log_recod_list, self.dictionary_of_search_value["THREAD"], self.initializedPath_object, self.outputDirectory_object)
                daemonLog_object.get_tomcat_log()
                if daemonLog_object.tomcat_thread_outfile.exists():
                    if self.is_error_tlog:
                        self.find_issue_daemon_log(daemonLog_object.tomcat_thread_outfile, TlogErrorTag)
                        
                    elif self.is_lowbal_tlog:
                        self.find_issue_daemon_log(daemonLog_object.tomcat_thread_outfile, TlogLowBalTag)
                                    
                    elif self.is_retry_tlog:
                        self.find_issue_daemon_log(daemonLog_object.tomcat_thread_outfile, TlogRetryTag)
                    
                    elif self.is_nhf_tlog:
                        self.find_issue_daemon_log(daemonLog_object.tomcat_thread_outfile, TlogNHFTag)
                    
                    elif self.is_handler_exp:
                        self.find_issue_daemon_log(daemonLog_object.tomcat_thread_outfile, TlogHandlerExp)
                    
                    
                    if self.is_issue_in_thread:
                
                        for ttype in TaskType:
                            with open(daemonLog_object.tomcat_thread_outfile, "r") as read_file:
                                for i, line in enumerate(read_file):
                                    if self.task == ttype.name:
                                        self.set_task_type(ttype.value)
                                        break
                                    
                        if self.is_nhf_tlog:
                            self.set_final_index(self.get_initial_index() - 1)
                        else:
                            with open(daemonLog_object.tomcat_thread_outfile, "r") as read_file:
                                if self.get_task_type() == "S":
                                    serach_string = f'-process handler params for task {self.get_task_type()} for subType:A'
                                else:
                                    serach_string = f'-process handler params for task {self.get_task_type()} for subType:{self.dictionary_of_search_value["SUB_TYPE"]}'

                                for i, line in enumerate(read_file):
                                    if re.search(r"{}".format(str(serach_string)), line):
                                        self.set_final_index(i)
                                        break
                        
                        log_writer.write_trimmed_thread_log(daemonLog_object.tomcat_thread_outfile, self.trimmed_tomcat_outfile, self.get_initial_index(), self.get_final_index())

                    else:
                        logging.debug('%s present without containing the issue tag.', self.dictionary_of_search_value["THREAD"])
                else:
                    logging.debug("Tomcat daemon log doesn't exist for the issue thread %s : ", self.dictionary_of_search_value["THREAD"])
            else:
                logging.debug('Transaction is awaiting notification callback. Hence not processing further.Ignore below logs.')
                
        if len(self.issue_tlog_data_prism) != 0:
            if not self.is_timeout_tlog and not self.is_await_push_tlog:
                
                logging.info('Going to fetch prism daemon log for the issue thread : %s', self.dictionary_of_search_value["THREAD"])
                daemonLog_object = DaemonLog(self.input_date, self.worker_log_recod_list, self.dictionary_of_search_value["THREAD"], self.initializedPath_object, self.outputDirectory_object)
                daemonLog_object.get_prism_log()
                if daemonLog_object.prismd_thread_outfile.exists():
                    if self.is_error_tlog:
                        self.find_issue_daemon_log(daemonLog_object.prismd_thread_outfile, TlogErrorTag)
                                    
                    elif self.is_lowbal_tlog:
                        self.find_issue_daemon_log(daemonLog_object.prismd_thread_outfile, TlogLowBalTag)
                                    
                    elif self.is_retry_tlog:
                        self.find_issue_daemon_log(daemonLog_object.prismd_thread_outfile, TlogRetryTag)
                    
                    elif self.is_nhf_tlog:
                        self.find_issue_daemon_log(daemonLog_object.prismd_thread_outfile, TlogNHFTag)
                    
                    elif self.is_await_push_tlog:
                        self.find_issue_daemon_log(daemonLog_object.prismd_thread_outfile, TlogAwaitPushTag)
                    
                    if self.is_issue_in_thread:
                        logging.info('is issue thread:%s', self.is_issue_in_thread)
                                       
                        for ttype in TaskType:
                            with open(daemonLog_object.prismd_thread_outfile, "r") as read_file:
                                for i, line in enumerate(read_file):
                                    if self.task == ttype.name:
                                        self.set_task_type(ttype.value)
                                        break
                        
                        if self.is_nhf_tlog:
                            self.set_final_index(self.get_initial_index() - 1)
                        else:
                            with open(daemonLog_object.prismd_thread_outfile, "r") as read_file:
                                if self.get_task_type() == "S":
                                    serach_string = f'-process handler params for task {self.get_task_type()} for subType:A'
                                else:
                                    serach_string = f'-process handler params for task {self.get_task_type()} for subType:{self.dictionary_of_search_value["SUB_TYPE"]}'
                                logging.info('search string: %s', serach_string)
                                for i, line in enumerate(read_file):
                                    if re.search(r"{}".format(str(serach_string)), line):
                                        self.set_final_index(i)
                                        break
                            
                        log_writer.write_trimmed_thread_log(daemonLog_object.prismd_thread_outfile, self.trimmed_prism_outfile, self.get_initial_index(), self.get_final_index())

                    else:
                        logging.debug('%s present without containing the issue tag.', self.dictionary_of_search_value["THREAD"])
                        
                else:
                    logging.debug("Prism daemon log doesn't exist for the issue thread %s : ", self.dictionary_of_search_value["THREAD"])
            else:
                logging.debug('Eigther transaction is in await push state or timed out.')
                logging.debug('Check for notification callback. Daemon log processing not required.')
    
    def find_issue_daemon_log(self, outfile, tlogTags):
        for status in tlogTags:
            with open(outfile, "r") as read_file:
                for i, line in enumerate(read_file):
                    if re.search(r"\b{}\b".format(str(status.value)), line):
                        self.set_initial_index(i)
                        self.task = status.name
                        self.is_issue_in_thread = True
                        break
                    
    def get_initial_index(self):
        """
        Get initial index from target file.
        """
        return self.__initial_index


    def set_initial_index(self, initial_index):
        """
        Setting initial index from
        """
        self.__initial_index = initial_index

    def get_final_index(self):
        """
        Get initial index from target file.
        """
        return self.__final_index


    def set_final_index(self, final_index):
        """
        Setting initial index from
        """
        self.__final_index = final_index

    def get_task_type(self):
        """
        Getting failure task type.
        """
        return self.__task_type


    def set_task_type(self, t_type):
        """
        Setting setting failure task type
        """
        self.__task_type = t_type