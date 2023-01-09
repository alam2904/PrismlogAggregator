"""
importing required modules
"""
import json
import logging
from pathlib import Path
import socket
import subprocess
import signal
from datetime import datetime
import re
import os
from daemon_log import DaemonLog
from outfile_writer import FileWriter
from configparser import ConfigParser
from tlog_tag import TaskType, TlogAwaitPushTag, TlogAwaitPushTimeOutTag, TlogErrorTag, TlogHandlerExp, TlogLowBalTag, TlogRetryTag, TlogNHFTag, TlogSmsTag

class TDLogParser:
    """
    Parse the daemon log based on tlog input
    """
    def __init__(self, msisdn, input_date, dictionary_of_tlogs, dictionary_of_search_value, worker_log_recod_list, initializedPath_object, tomcat_thread_outfile, prismd_thread_outfile, smsd_thread_outfile, trimmed_tomcat_outfile, trimmed_prism_outfile, issue_tlog_path, dictionary_tlog_to_search, dict_key):
        self.msisdn = msisdn
        self.input_date = input_date
        self.initializedPath_object = initializedPath_object
        self.dictionary_of_tlogs = dictionary_of_tlogs
        self.dictionary_of_search_value = dictionary_of_search_value
        self.worker_log_recod_list = worker_log_recod_list
        self.dictionary_tlog_to_search = dictionary_tlog_to_search

        self.__initial_index = 0
        self.__final_index = 0
        self.__task_type = ""

        self.tomcat_thread_outfile = tomcat_thread_outfile
        self.prismd_thread_outfile = prismd_thread_outfile
        self.smsd_thread_outfile = smsd_thread_outfile
        self.trimmed_tomcat_outfile = trimmed_tomcat_outfile
        self.trimmed_prism_outfile = trimmed_prism_outfile
        self.issue_tlog_path = issue_tlog_path
        # self.file = file
        
        self.issue_tlog_data_prism = ""
        self.issue_tlog_data_tomcat = ""
        self.issue_tlog_data_sms = ""
        self.issue_plog_data_prism = ""
        self.issue_plog_data_tomcat = ""
        
        self.task = ""
        self.acc_log = ""
        self.new_line = '\n'
        self.is_issue_in_thread = False
        self.dict_key = dict_key


    def parse(self, tlogParser_object, msisdn, key, value, is_error_tlog, is_lowbal_tlog, is_retry_tlog, is_nhf_tlog, is_await_push_tlog, is_timeout_tlog, is_handler_exp):
        """
        Parse dictionary of tlogs to get the search value.
        """
        logging.info('Going to parse tlog for ERROR/RETRY/LOWBAL/HNF/AWAIT_PUSH/AWAIT_PUSH_TIMEOUT cases.')
        
        log_writer = FileWriter()
            
        is_error_tlog = self.parse_tlog(TlogErrorTag, is_error_tlog, key, value)    
        
        if not is_error_tlog:
            is_lowbal_tlog = self.parse_tlog(TlogLowBalTag, is_lowbal_tlog, key, value)
        
        if not is_lowbal_tlog:
            is_retry_tlog = self.parse_tlog(TlogRetryTag, is_retry_tlog, key, value)
        
        if not is_retry_tlog:
            is_nhf_tlog = self.parse_tlog(TlogNHFTag, is_nhf_tlog, key, value)
        
        if not is_nhf_tlog:
            is_await_push_tlog = self.parse_tlog(TlogAwaitPushTag, is_await_push_tlog, key, value)
        
        if not is_await_push_tlog:
            is_timeout_tlog = self.parse_tlog(TlogAwaitPushTimeOutTag, is_timeout_tlog, key, value)
        
        if not is_timeout_tlog:
            is_handler_exp = self.parse_tlog(TlogHandlerExp, is_handler_exp, key, value)
        
        logging.info('dictionary to search: %s', self.dictionary_tlog_to_search)
        if self.dictionary_tlog_to_search and self.dict_key:       
            
            logging.debug('issue tlog found for given msisdn: %s and worker thread: %s', msisdn, self.dictionary_tlog_to_search[self.dict_key]["THREAD"])
        
            try:
                if tlogParser_object.filtered_prism_tlog:
                        self.process_prism_log(tlogParser_object, log_writer, msisdn)

                elif tlogParser_object.filtered_tomcat_tlog:
                    self.process_tomcat_log(tlogParser_object, log_writer, msisdn)
                
                logging.info('key to iterate trimmed log: %s', self.dict_key)
                
                self.get_trimmed_thread_log(is_error_tlog, is_lowbal_tlog, is_retry_tlog, is_nhf_tlog, is_await_push_tlog, is_timeout_tlog, is_handler_exp)
                self.dictionary_tlog_to_search = {}
                
            except ValueError as ex:
                logging.exception(ex)
        else:
            logging.debug('No issue tlog found for given msisdn: %s and worker thread: %s', msisdn, self.dictionary_of_tlogs[key]["THREAD"])
            logging.debug('Hence not fetching the daemon log. Will check if more issue tlogs present.')
            
    def process_prism_log(self, tlogParser_object, log_writer, msisdn):
        # tlog_index = self.dict_key.split("_")[1]
        tlog_index = self.dictionary_tlog_to_search[self.dict_key]["THREAD"]
        if self.initializedPath_object.is_access_path:
            access_path = self.initializedPath_object.tomcat_log_path_dict[self.initializedPath_object.tomcat_access_path]
            logging.info('Issue tlog found. Going to fetch access log.')
        
        if self.dictionary_tlog_to_search[self.dict_key]["CHARGE_TYPE"] == 'A':
            acc_log = self.fetch_access_log(msisdn, "/subscription/ActivateSubscription?", access_path)
        elif self.dictionary_tlog_to_search[self.dict_key]["CHARGE_TYPE"] == 'D':
            acc_log = self.fetch_access_log(msisdn, "/subscription/DeactivateSubscription?", access_path)
        elif self.dictionary_tlog_to_search[self.dict_key]["CHARGE_TYPE"] == 'E':
            acc_log = self.fetch_access_log(msisdn, "/subscription/EventCharge?", access_path)
        elif self.dictionary_tlog_to_search[self.dict_key]["CHARGE_TYPE"] == 'U':
            acc_log = self.fetch_access_log(msisdn, "/subscription/UpgradeSubscription?", access_path)
        elif self.dictionary_tlog_to_search[self.dict_key]["CHARGE_TYPE"] == 'T':
            acc_log = self.fetch_access_log(msisdn, "/subscription/TriggerCharge?", access_path)
        elif self.dictionary_tlog_to_search[self.dict_key]["CHARGE_TYPE"] == 'G':
            acc_log = self.fetch_access_log(msisdn, "/subscription/ChargeGift?", access_path)
        elif self.dictionary_tlog_to_search[self.dict_key]["CHARGE_TYPE"] == 'V':
            acc_log = self.fetch_access_log(msisdn, "/subscription/AddRenewalTrigger?", access_path)

        if not tlogParser_object.filtered_tomcat_tlog:
            if os.path.isfile(self.issue_tlog_path) and os.path.getsize(self.issue_tlog_path) != 0:
                os.remove(self.issue_tlog_path)
                
        if acc_log:
            logging.info('access log to write: %s', acc_log)
            log_writer.write_access_log(self.issue_tlog_path, f"NON_REALTIME_ACCESS_LOG\n", tlog_index)
            log_writer.write_issue_tlog(self.issue_tlog_path, f"**************************\n", tlog_index)
            log_writer.write_access_log(self.issue_tlog_path, acc_log, tlog_index)
        
        for data in tlogParser_object.filtered_prism_tlog:
            if self.dictionary_tlog_to_search[self.dict_key]["THREAD"] == data.split("|")[1]:
                self.issue_tlog_data_prism = data
                log_writer.write_issue_tlog(self.issue_tlog_path, f"\n", tlog_index)
                log_writer.write_issue_tlog(self.issue_tlog_path, f"PRISM_TLOG\n", tlog_index)
                log_writer.write_issue_tlog(self.issue_tlog_path, f"***************\n", tlog_index)
                log_writer.write_issue_tlog(self.issue_tlog_path, self.issue_tlog_data_prism, tlog_index)
                break
            
        if tlogParser_object.filtered_prism_plog:
            for data in tlogParser_object.filtered_prism_plog:
                if self.dictionary_tlog_to_search[self.dict_key]["THREAD"] == data.split("|")[1]:
                    self.issue_plog_data_prism = data
                    log_writer.write_issue_plog(self.issue_tlog_path, f"\n", tlog_index)
                    log_writer.write_issue_plog(self.issue_tlog_path, f"\nPRISM_PERF_LOG\n", tlog_index)
                    log_writer.write_issue_plog(self.issue_tlog_path, f"*****************\n", tlog_index)
                    log_writer.write_issue_plog(self.issue_tlog_path, self.issue_plog_data_prism, tlog_index)
                    break
        else:
            logging.info("worker thread: %s could not be found in prism perf log.")
        
    def process_tomcat_log(self, tlogParser_object, log_writer, msisdn):
        # tlog_index = self.dict_key.split("_")[1]
        tlog_index = self.dictionary_tlog_to_search[self.dict_key]["THREAD"]
        if self.initializedPath_object.is_access_path:
            access_path = self.initializedPath_object.tomcat_log_path_dict[self.initializedPath_object.tomcat_access_path]
            logging.info('Issue tlog found. Going to fetch access log.')
            
            if self.dictionary_tlog_to_search[self.dict_key]["CHARGE_TYPE"] == 'A':
                acc_log = self.fetch_access_log(msisdn, "/subscription/RealTimeActivate?", access_path)
            elif self.dictionary_tlog_to_search[self.dict_key]["CHARGE_TYPE"] == 'D':
                acc_log = self.fetch_access_log(msisdn, "/subscription/RealTimeDeactivate?", access_path)
            elif self.dictionary_tlog_to_search[self.dict_key]["CHARGE_TYPE"] == 'E':
                acc_log = self.fetch_access_log(msisdn, "/subscription/RealTimeCharge?", access_path)
            elif self.dictionary_tlog_to_search[self.dict_key]["CHARGE_TYPE"] == 'F':
                acc_log = self.fetch_access_log(msisdn, "/subscription/RealTimeTransactionRefund?", access_path)
        
        if os.path.isfile(self.issue_tlog_path) and os.path.getsize(self.issue_tlog_path) != 0:
            os.remove(self.issue_tlog_path)
        
        if acc_log:
            logging.info('access log to write: %s', acc_log)
            log_writer.write_access_log(self.issue_tlog_path, f"REALTIME_ACCESS_LOG\n", tlog_index)
            log_writer.write_issue_tlog(self.issue_tlog_path, f"***********************\n", tlog_index)
            log_writer.write_access_log(self.issue_tlog_path, acc_log, tlog_index)
        
        for data in tlogParser_object.filtered_tomcat_tlog:
            if self.dictionary_tlog_to_search[self.dict_key]["THREAD"] == data.split("|")[1]:
                self.issue_tlog_data_tomcat = data
                log_writer.write_issue_tlog(self.issue_tlog_path, f"\n", tlog_index)
                log_writer.write_issue_tlog(self.issue_tlog_path, f"TOMCAT_TLOG\n", tlog_index)
                log_writer.write_issue_tlog(self.issue_tlog_path, f"************\n", tlog_index)
                log_writer.write_issue_tlog(self.issue_tlog_path, self.issue_tlog_data_tomcat, tlog_index)
                break
            
        if tlogParser_object.filtered_tomcat_plog:
            for data in tlogParser_object.filtered_tomcat_plog:
                if self.dictionary_tlog_to_search[self.dict_key]["THREAD"] == data.split("|")[1]:
                    self.issue_plog_data_tomcat = data
                    log_writer.write_issue_plog(self.issue_tlog_path, f"\n", tlog_index)
                    log_writer.write_issue_plog(self.issue_tlog_path, f"\nTOMCAT_PERF_LOG\n", tlog_index)
                    log_writer.write_issue_plog(self.issue_tlog_path, f"*****************\n", tlog_index)
                    log_writer.write_issue_plog(self.issue_tlog_path, self.issue_plog_data_tomcat, tlog_index)
                    break
        else:
            logging.info("worker thread: %s could not be found in tomcat perf log.")
    
    def parse_sms_td(self, tlogParser_object, msisdn, key, is_issue_sms_tlog):
        """
        Parse dictionary of tlogs to get the search value.
        """
        logging.info('Going to parse sms tlog for INVALID/RETRY_EXCEEDED/PENDING/SUSPENDED/QUEUED cases.')
        
        log_writer = FileWriter()
        # log_writer = FileWriter()
        
        is_issue_sms_tlog = self.parse_sms_tlog(TlogSmsTag, is_issue_sms_tlog, key)
        
        logging.info('dictionary to search: %s', self.dictionary_tlog_to_search)
        if self.dictionary_tlog_to_search and is_issue_sms_tlog and self.dict_key:
            
            logging.debug('issue tlog found for given msisdn: %s and worker thread: %s', msisdn, self.dictionary_tlog_to_search[self.dict_key]["THREAD"])
            try:
                if tlogParser_object.filtered_sms_tlog:
                    self.process_sms_log(tlogParser_object, log_writer)
                    
                logging.info('key to iterate trimmed log: %s', self.dict_key)
                
                self.get_trimmed_thread_log_sms(is_issue_sms_tlog)
                self.dictionary_tlog_to_search = {}
            
            except ValueError as ex:
                logging.exception(ex)
        
        else:
            logging.debug('No sms issue tlog found for given msisdn: %s', msisdn)
            logging.debug('Hence not fetching the sms daemon log.')
    
    def process_sms_log(self, tlogParser_object, log_writer):
        # tlog_index = self.dict_key.split("_")[1]
        tlog_index = self.dictionary_tlog_to_search[self.dict_key]["THREAD"]
        
        for data in tlogParser_object.filtered_sms_tlog:
            if self.dictionary_tlog_to_search[self.dict_key]["THREAD"] == data.split("|")[1]:
                self.issue_tlog_data_sms = data
                log_writer.write_issue_tlog(self.issue_tlog_path, f"\n", tlog_index)
                log_writer.write_issue_tlog(self.issue_tlog_path, f"SMS_TLOG\n", tlog_index)
                log_writer.write_issue_tlog(self.issue_tlog_path, f"***************\n", tlog_index)
                log_writer.write_issue_tlog(self.issue_tlog_path, self.issue_tlog_data_sms, tlog_index)
                break
    
    def fetch_access_log(self, msisdn, search_string, access_path):
        # config = ConfigParser()
        # config.read(self.file)
        acc_log = ""
        
        try:
            hostname = socket.gethostname()
            data = Path("common.json").read_text()
            config = json.loads(data)
            if config[hostname]['PRISM']['PRISM_TOMCAT']['PREFIX'] != "" and config[hostname]['PRISM']['PRISM_TOMCAT']['SUFFIX'] != "":
                access_log = subprocess.check_output(f"grep {search_string} {access_path}/{config[hostname]['PRISM']['PRISM_TOMCAT']['PREFIX']}*.{config[hostname]['PRISM']['PRISM_TOMCAT']['SUFFIX']}", universal_newlines=True, shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
                for record in access_log.splitlines():
                    if re.search(msisdn,record, re.DOTALL):
                        temp_record = f'{record.split("- -")[1]}'
                        data = [data.split("refid=")[1].split("&")[0] for data in temp_record.split(",") if data.split("refid=")]
                        
                        for refid in str(self.dictionary_tlog_to_search[self.dict_key]).split(","):
                            if f'refId={data[0]}' == refid.split("]")[0] and data[0] != "1":
                                acc_log = f'{record.split("- -")[1].strip()}{self.new_line}'
                            
                            elif data[0] == "1":
                                a_date = [temp.split("]")[0].split("[")[1].split(" ")[0].split(":") for temp in temp_record.split(",")]
                                acc_date = f'{a_date[0][0]}{a_date[0][1]}:{a_date[0][2]}'
                                acc_log_date = datetime.strptime(acc_date, "%d/%b/%Y%H:%M")

                                r_date = str(self.dictionary_tlog_to_search[self.dict_key]["TIMESTAMP"].split(",")[0]).split(":")
                                req_date = f'{r_date[0]}:{r_date[1]}'
                                tlog_req_date = datetime.strptime(req_date, "%Y-%m-%d %H:%M")
                                
                                if acc_log_date == tlog_req_date:
                                    acc_log = f'{record.split("- -")[1].strip()}{self.new_line}'
                return acc_log
            else:
                logging.error('tomcat access log path, PREFIX or SUFFIX not present, hence access log could not be fetched.')
                 
        except subprocess.CalledProcessError as ex:            
            logging.info('No access log found')

    def parse_tlog(self, tlogTags, is_tlog, key, value):
        for keyy, valuee in value.items():
            for status in tlogTags:
                # if re.search(status.value,valuee, re.DOTALL):
                if re.search(r"\b{}\b".format(str(status.value)), valuee):
                    self.dictionary_tlog_to_search[key] = self.dictionary_of_tlogs[key]
                    is_tlog = True
                    self.dict_key = key
                    logging.info('dict of search value: %s', self.dictionary_tlog_to_search[key])
        return is_tlog
    
    def parse_sms_tlog(self, tlogSmsTags, is_tlog, key):
        for status in tlogSmsTags:
            if status.value == self.dictionary_of_tlogs[key]["STATUS"]:
                self.dictionary_tlog_to_search[key] = self.dictionary_of_tlogs[key]
                is_tlog = True
                self.dict_key = key
                logging.info('dict of search value: %s', self.dictionary_tlog_to_search[key])
        return is_tlog
            
    def get_trimmed_thread_log(self, is_error_tlog, is_lowbal_tlog, is_retry_tlog, is_nhf_tlog, is_await_push_tlog, is_timeout_tlog, is_handler_exp):
        """
        Get daemon log for the given thread
        """
        log_writer = FileWriter()
        # tlog_index = self.dict_key.split("_")[1]
        tlog_index = self.dictionary_tlog_to_search[self.dict_key]["THREAD"]
        
        self.tomcat_thread_outfile = f'{self.tomcat_thread_outfile.split(".log")[0]}_{tlog_index}.log'
        self.prismd_thread_outfile = f'{self.prismd_thread_outfile.split(".log")[0]}_{tlog_index}.log'
        self.trimmed_tomcat_outfile = f'{self.trimmed_tomcat_outfile.split(".log")[0]}_{tlog_index}.log'
        self.trimmed_prism_outfile = f'{self.trimmed_prism_outfile.split(".log")[0]}_{tlog_index}.log'
        
        if len(self.issue_tlog_data_tomcat) != 0:
            logging.info('trimmed thread: %s', self.dictionary_tlog_to_search[self.dict_key]["THREAD"])
            logging.info('is await push: %s', is_await_push_tlog)
            
            if not is_await_push_tlog:
                logging.info('Going to fetch daemon log for the issue thread : %s', self.dictionary_tlog_to_search[self.dict_key]["THREAD"])
                
                daemonLog_object = DaemonLog(self.msisdn, self.input_date, self.worker_log_recod_list, self.dictionary_tlog_to_search[self.dict_key]["THREAD"], self.initializedPath_object, self.tomcat_thread_outfile, self.prismd_thread_outfile, self.smsd_thread_outfile)
                daemonLog_object.get_tomcat_log()
                
                if self.tomcat_thread_outfile:
                    
                    if is_error_tlog:
                        self.find_issue_daemon_log(self.tomcat_thread_outfile, TlogErrorTag)
                    elif is_lowbal_tlog:
                        self.find_issue_daemon_log(self.tomcat_thread_outfile, TlogLowBalTag)
                    elif is_retry_tlog:
                        self.find_issue_daemon_log(self.tomcat_thread_outfile, TlogRetryTag)
                    elif is_nhf_tlog:
                        self.find_issue_daemon_log(self.tomcat_thread_outfile, TlogNHFTag)
                    elif is_handler_exp:
                        self.find_issue_daemon_log(self.tomcat_thread_outfile, TlogHandlerExp)
                        
                    logging.info('is issue thread: %s', self.is_issue_in_thread)
                    if self.is_issue_in_thread:
                
                        for ttype in TaskType:
                            with open(self.tomcat_thread_outfile, "r") as read_file:
                                for i, line in enumerate(read_file):
                                    if self.task == ttype.name:
                                        self.set_task_type(ttype.value)
                                        break
                                    
                        if is_nhf_tlog:
                            self.set_final_index(self.get_initial_index() - 1)
                        else:
                            with open(self.tomcat_thread_outfile, "r") as read_file:
                                if self.get_task_type() == "S":
                                    serach_string = f'-process handler params for task {self.get_task_type()} for subType:A'
                                else:
                                    serach_string = f'-process handler params for task {self.get_task_type()} for subType:{self.dictionary_tlog_to_search[self.dict_key]["SUB_TYPE"]}'

                                for i, line in enumerate(read_file):
                                    if re.search(r"{}".format(str(serach_string)), line):
                                        self.set_final_index(i)
                                        break
                        
                        log_writer.write_trimmed_thread_log(self.tomcat_thread_outfile, self.trimmed_tomcat_outfile, self.get_initial_index(), self.get_final_index())

                    else:
                        logging.debug('%s present without containing the issue tag.', self.dictionary_tlog_to_search[self.dict_key]["THREAD"])
                else:
                    logging.debug("Tomcat daemon log doesn't exist for the issue thread %s : ", self.dictionary_tlog_to_search[self.dict_key]["THREAD"])
            else:
                logging.debug('Transaction is awaiting notification callback. Hence not processing further.Ignore below logs.')
          
        if len(self.issue_tlog_data_prism) != 0:
                
            if not is_timeout_tlog:
                if not is_await_push_tlog:  
                    logging.info('Going to fetch daemon log for the issue thread : %s', self.dictionary_tlog_to_search[self.dict_key]["THREAD"])
                    
                    daemonLog_object = DaemonLog(self.msisdn, self.input_date, self.worker_log_recod_list, self.dictionary_tlog_to_search[self.dict_key]["THREAD"], self.initializedPath_object, self.tomcat_thread_outfile, self.prismd_thread_outfile, self.smsd_thread_outfile)
                    daemonLog_object.get_prism_log()
                    
                    if self.prismd_thread_outfile:
                        
                        if is_error_tlog:
                                self.find_issue_daemon_log(self.prismd_thread_outfile, TlogErrorTag)
                        elif is_lowbal_tlog:
                            self.find_issue_daemon_log(self.prismd_thread_outfile, TlogLowBalTag)           
                        elif is_retry_tlog:
                            self.find_issue_daemon_log(self.prismd_thread_outfile, TlogRetryTag)
                        elif is_nhf_tlog:
                            self.find_issue_daemon_log(self.prismd_thread_outfile, TlogNHFTag)
                        elif is_handler_exp:
                            self.find_issue_daemon_log(self.prismd_thread_outfile, TlogHandlerExp)
                
                        logging.info('is issue thread:%s', self.is_issue_in_thread)
                        if self.is_issue_in_thread:
                                        
                            for ttype in TaskType:
                                with open(self.prismd_thread_outfile, "r") as read_file:
                                    for i, line in enumerate(read_file):
                                        if self.task == ttype.name:
                                            self.set_task_type(ttype.value)
                                            break
                            
                            if is_nhf_tlog:
                                self.set_final_index(self.get_initial_index() - 1)
                            else:
                                with open(self.prismd_thread_outfile, "r") as read_file:
                                    if self.get_task_type() == "S":
                                        serach_string = f'-process handler params for task {self.get_task_type()} for subType:A'
                                    else:
                                        serach_string = f'-process handler params for task {self.get_task_type()} for subType:{self.dictionary_tlog_to_search[self.dict_key]["SUB_TYPE"]}'
                                    
                                    logging.info('search string: %s', serach_string)
                                    for i, line in enumerate(read_file):
                                        if re.search(r"{}".format(str(serach_string)), line):
                                            self.set_final_index(i)
                                            break
                                
                            log_writer.write_trimmed_thread_log(self.prismd_thread_outfile, self.trimmed_prism_outfile, self.get_initial_index(), self.get_final_index())

                        else:
                            logging.debug('%s present without containing the issue tag.', self.dictionary_tlog_to_search[self.dict_key]["THREAD"])
                                
                else:
                    logging.info('Since issue thread : %s is in await push state, not going to fetch log', self.dictionary_tlog_to_search[self.dict_key]["THREAD"])
                    logging.debug('Check for notification callback. Daemon log processing not required.')
            else:
                logging.info('Since issue thread : %s is in timed out state, not going to fetch log', self.dictionary_tlog_to_search[self.dict_key]["THREAD"])
                logging.debug('Check for notification callback eigther not received before timed out or failed to process.')
        else:
            logging.debug("daemon log doesn't exist for the issue thread %s : ", self.dictionary_tlog_to_search[self.dict_key]["THREAD"])
        
    
    def get_trimmed_thread_log_sms(self, is_issue_sms_tlog):
        """
        Get sms daemon log for the given thread
        """
        log_writer = FileWriter()
        # tlog_index = self.dict_key.split("_")[1]
        tlog_index = self.dictionary_tlog_to_search[self.dict_key]["THREAD"]    
        self.smsd_thread_outfile = f'{self.smsd_thread_outfile.split(".log")[0]}_{tlog_index}.log'
        
        if len(self.issue_tlog_data_sms) != 0 and is_issue_sms_tlog:        
            logging.info('Going to fetch sms daemon log for the issue thread : %s', self.dictionary_tlog_to_search[self.dict_key]["THREAD"])
    
            daemonLog_object = DaemonLog(self.msisdn, self.input_date, self.worker_log_recod_list, self.dictionary_tlog_to_search[self.dict_key]["THREAD"], self.initializedPath_object, self.tomcat_thread_outfile, self.prismd_thread_outfile, self.smsd_thread_outfile)
            daemonLog_object.get_sms_log()
        else:
            logging.debug("Sms daemon log doesn't exist for the issue thread %s : ", self.dictionary_tlog_to_search[self.dict_key]["THREAD"])
    
    def find_issue_daemon_log(self, outfile, tlogTags):
        try:
            for status in tlogTags:
                with open(outfile, "r") as read_file:
                    for i, line in enumerate(read_file):
                        if re.search(r"\b{}\b".format(str(status.value)), line):
                            self.set_initial_index(i)
                            self.task = status.name
                            self.is_issue_in_thread = True
                            break
        except FileNotFoundError as ex:
            logging.info('No out file generated for daemon log. Hence log trimming could not be done.')
                    
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