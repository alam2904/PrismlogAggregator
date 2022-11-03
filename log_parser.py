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
from tlog_tag import TaskType, TlogErrorTag, TlogLowBalTag, TlogRetryTag

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
        self.outputDirectory_object = outputDirectory_object
        self.trimmed_prism_outfile = self.outputDirectory_object/"trimmed_prismd.log"
        self.trimmed_tomcat_outfile = self.outputDirectory_object/"trimmed_tomcat.log"
        self.issue_tlog = self.outputDirectory_object/"issue_tlog_record.txt"
        self.issue_tlog_data_prism = ""
        self.issue_tlog_data_tomcat = ""
        self.is_error_tlog = False
        self.is_lowbal_tlog = False
        self.is_retry_tlog = False
        self.task = ""

    def parse(self, tlogParser_object, msisdn):
        """
        Parse dictionary of tlogs to get the search value.
        """
        logging.debug('Parsing tlog and daemon log')

        for key, value in self.dictionary_of_tlogs.items():
            for status in TlogErrorTag:
                if re.search(r"\b{}\b".format(str(status.value)), value):
                    for search_key, search_value in self.dictionary_of_search_value.items():
                        self.dictionary_of_search_value[search_key] = self.dictionary_of_tlogs[search_key]
                        self.is_error_tlog = True
                    break
                        
        if not self.is_error_tlog:
            for key, value in self.dictionary_of_tlogs.items():
                for status in TlogLowBalTag:
                    if re.search(r"\b{}\b".format(str(status.value)), value):
                        for search_key, search_value in self.dictionary_of_search_value.items():
                            self.dictionary_of_search_value[search_key] = self.dictionary_of_tlogs[search_key]
                            self.is_lowbal_tlog = True
                        break
        
        if not self.is_lowbal_tlog:
            for key, value in self.dictionary_of_tlogs.items():
                for status in TlogRetryTag:
                    if re.search(r"\b{}\b".format(str(status.value)), value):
                        for search_key, search_value in self.dictionary_of_search_value.items():
                            self.dictionary_of_search_value[search_key] = self.dictionary_of_tlogs[search_key]
                            self.is_retry_tlog = True
                        break
        
                                      
        if tlogParser_object.filtered_prism_tlog:
            access_path = self.initializedPath_object.tomcat_log_path_dict[self.initializedPath_object.tomcat_access_path]
            dts = datetime.strptime(self.input_date, "%Y%m%d")
            dtf = dts.strftime("%Y-%m-%d")
            date_formated = dtf.split("-")

            try:
                access_log = subprocess.run(["grep", f"/subscription/ActivateSubscription?msisdn={msisdn}", f"{access_path}/localhost_access_log.{date_formated[0]}-{date_formated[1]}-{date_formated[2]}.txt"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
                acc_log = [data for data in access_log.stdout]
                
                with open(self.issue_tlog, "a") as write_file:
                    write_file.writelines(acc_log)
                    
            except subprocess.CalledProcessError as ex:
                try: 
                    access_log = subprocess.run(["grep", f"/subscription/ActivateSubscription?msisdn={msisdn}", f"{self.initializedPath_object.dict_of_process_dir['tomcat']['PROCESS_HOME_DIR']}/{access_path}/localhost_access_log.{date_formated[0]}-{date_formated[1]}-{date_formated[2]}.txt"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
                    acc_log = [data for data in access_log.stdout]
                    
                    with open(self.issue_tlog, "a") as write_file:
                        write_file.writelines(acc_log)
                except subprocess.CalledProcessError as ex:
                    logging.info('No access log found') 
                                
            with open(self.issue_tlog, "a") as write_file:
                self.issue_tlog_data_prism = tlogParser_object.filtered_prism_tlog[-1]
                write_file.writelines(self.issue_tlog_data_prism)
            
        
        elif tlogParser_object.filtered_tomcat_tlog:
            access_path = self.initializedPath_object.tomcat_log_path_dict[self.initializedPath_object.tomcat_access_path]
            dts = datetime.strptime(self.input_date, "%Y%m%d")
            dtf = dts.strftime("%Y-%m-%d")
            date_formated = dtf.split("-")

            try:
                access_log = subprocess.run(["grep", f"/subscription/RealTimeActivate?msisdn={msisdn}", f"{access_path}/localhost_access_log.{date_formated[0]}-{date_formated[1]}-{date_formated[2]}.txt"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
                acc_log = [data for data in access_log.stdout]
                
                with open(self.issue_tlog, "a") as write_file:
                    write_file.writelines(acc_log)
                    
            except subprocess.CalledProcessError as ex:
                logging.info('No access log found') 
                
                
            with open(self.issue_tlog, "a") as write_file:
                self.issue_tlog_data_tomcat = tlogParser_object.filtered_tomcat_tlog[-1]
                write_file.writelines(self.issue_tlog_data_tomcat)
                    

        self.get_serched_log()

    def get_serched_log(self):
        """
        Get daemon log for the given thread
        """

        # task = ""
        if len(self.issue_tlog_data_tomcat) != 0:
            logging.debug('Getting daemon log for the issue thread : %s', self.dictionary_of_search_value["THREAD"])
            daemonLog_object = DaemonLog(self.input_date, self.worker_log_recod_list, self.dictionary_of_search_value["THREAD"], self.initializedPath_object, self.outputDirectory_object)
            daemonLog_object.get_tomcat_log()
            if daemonLog_object.tomcat_thread_outfile.exists():
                if self.is_error_tlog:
                    for status in TlogErrorTag:
                        with open(daemonLog_object.tomcat_thread_outfile, "r") as read_file:
                            for i, line in enumerate(read_file):
                                if re.search(r"\b{}\b".format(str(status.value)), line):
                                    self.set_initial_index(i)
                                    self.task = status.name
                                    break
                elif self.is_lowbal_tlog:
                    for status in TlogLowBalTag:
                        with open(daemonLog_object.tomcat_thread_outfile, "r") as read_file:
                            for i, line in enumerate(read_file):
                                if re.search(r"\b{}\b".format(str(status.value)), line):
                                    self.set_initial_index(i)
                                    self.task = status.name
                                    break
                                
                elif self.is_retry_tlog:
                    for status in TlogRetryTag:
                        with open(daemonLog_object.tomcat_thread_outfile, "r") as read_file:
                            for i, line in enumerate(read_file):
                                if re.search(r"\b{}\b".format(str(status.value)), line):
                                    self.set_initial_index(i)
                                    self.task = status.name
                                    break
                    
            
                for ttype in TaskType:
                    with open(daemonLog_object.tomcat_thread_outfile, "r") as read_file:
                        for i, line in enumerate(read_file):
                            if self.task == ttype.name:
                                self.set_task_type(ttype.value)
                                break

                with open(daemonLog_object.tomcat_thread_outfile, "r") as read_file:
                    serach_string = f'-process handler params for task {self.get_task_type()} for subType:{self.dictionary_of_search_value["SUB_TYPE"]}'

                    for i, line in enumerate(read_file):
                        if re.search(r"{}".format(str(serach_string)), line):
                            self.set_final_index(i)
                            break
                
                with open(daemonLog_object.tomcat_thread_outfile, "r") as read_file:
                    for i, line in enumerate(read_file):
                        if self.get_final_index() <= i < self.get_initial_index() + 1:
                            with open(self.trimmed_tomcat_outfile, "a") as write_file:
                                write_file.writelines(line)
            else:
                logging.error("Tomcat daemon log doesn't exist for the issue thread %s : ", self.dictionary_of_search_value["THREAD"])
        
        if len(self.issue_tlog_data_prism) != 0:
            logging.debug('Getting daemon log for the issue thread : %s', self.dictionary_of_search_value["THREAD"])
            daemonLog_object = DaemonLog(self.input_date, self.worker_log_recod_list, self.dictionary_of_search_value["THREAD"], self.initializedPath_object, self.outputDirectory_object)
            daemonLog_object.get_prism_log()
            if daemonLog_object.prismd_thread_outfile.exists():
                if self.is_error_tlog:
                    for status in TlogErrorTag:
                        with open(daemonLog_object.prismd_thread_outfile, "r") as read_file:
                            for i, line in enumerate(read_file):
                                if re.search(r"\b{}\b".format(str(status.value)), line):
                                    self.set_initial_index(i)
                                    self.task = status.name
                                    break
                                
                elif self.is_lowbal_tlog:
                    for status in TlogLowBalTag:
                        with open(daemonLog_object.prismd_thread_outfile, "r") as read_file:
                            for i, line in enumerate(read_file):
                                if re.search(r"\b{}\b".format(str(status.value)), line):
                                    self.set_initial_index(i)
                                    self.task = status.name
                                    break
                                
                elif self.is_retry_tlog:
                    for status in TlogRetryTag:
                        with open(daemonLog_object.prismd_thread_outfile, "r") as read_file:
                            for i, line in enumerate(read_file):
                                if re.search(r"\b{}\b".format(str(status.value)), line):
                                    self.set_initial_index(i)
                                    self.task = status.name
                                    break
                    
            
                for ttype in TaskType:
                    with open(daemonLog_object.prismd_thread_outfile, "r") as read_file:
                        for i, line in enumerate(read_file):
                            if self.task == ttype.name:
                                self.set_task_type(ttype.value)
                                break

                with open(daemonLog_object.prismd_thread_outfile, "r") as read_file:
                    serach_string = f'-process handler params for task {self.get_task_type()} for subType:{self.dictionary_of_search_value["SUB_TYPE"]}'
                    for i, line in enumerate(read_file):
                        if re.search(r"{}".format(str(serach_string)), line):
                            self.set_final_index(i)
                            break
                
                with open(daemonLog_object.prismd_thread_outfile, "r") as read_file:
                    for i, line in enumerate(read_file):
                        if self.get_final_index() <= i < self.get_initial_index() + 1:
                            with open(self.trimmed_prism_outfile, "a") as write_file:
                                write_file.writelines(line)
            else:
                logging.error("Prism daemon log doesn't exist for the issue thread %s : ", self.dictionary_of_search_value["THREAD"])

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