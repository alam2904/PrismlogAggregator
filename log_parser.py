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
from tlog_tag import TaskType, TlogAwaitPushTag, TlogAwaitPushTimeOutTag, TlogErrorTag, TlogLowBalTag, TlogRetryTag, TlogNHFTag

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
        self.is_prism_processing_required = True
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
        self.task = ""
        self.acc_log = []
        self.new_line = '\n'
        self.is_issue_in_thread = False

    def parse(self, tlogParser_object, msisdn):
        """
        Parse dictionary of tlogs to get the search value.
        """
        logging.info('Going to parse tlog for ERROR/RETRY/LOWBAL/HNF/AWAIT_PUSH/AWAIT_PUSH_TIMEOUT cases.')
        
        dts = datetime.strptime(self.input_date, "%Y%m%d")
        dtf = dts.strftime("%Y-%m-%d")
        date_formated = dtf.split("-")
        
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
        
        if not self.is_retry_tlog:
            for key, value in self.dictionary_of_tlogs.items():
                for status in TlogNHFTag:
                    if re.search(r"\b{}\b".format(str(status.value)), value):
                        for search_key, search_value in self.dictionary_of_search_value.items():
                            self.dictionary_of_search_value[search_key] = self.dictionary_of_tlogs[search_key]
                            self.is_nhf_tlog = True
                        break
        
        if not self.is_nhf_tlog:
            for key, value in self.dictionary_of_tlogs.items():
                for status in TlogAwaitPushTag:
                    if re.search(r"\b{}\b".format(str(status.value)), value):
                        for search_key, search_value in self.dictionary_of_search_value.items():
                            self.dictionary_of_search_value[search_key] = self.dictionary_of_tlogs[search_key]
                            self.is_await_push_tlog = True
                        break
        
        if not self.is_await_push_tlog:
            for key, value in self.dictionary_of_tlogs.items():
                for status in TlogAwaitPushTimeOutTag:
                    if re.search(r"\b{}\b".format(str(status.value)), value):
                        for search_key, search_value in self.dictionary_of_search_value.items():
                            self.dictionary_of_search_value[search_key] = self.dictionary_of_tlogs[search_key]
                            self.is_timeout_tlog = True
                        break
                  
        
        if tlogParser_object.filtered_prism_tlog and self.is_prism_processing_required and (self.is_error_tlog or self.is_lowbal_tlog or self.is_retry_tlog or self.is_nhf_tlog or self.is_await_push_tlog or self.is_timeout_tlog):
            access_path = self.initializedPath_object.tomcat_log_path_dict[self.initializedPath_object.tomcat_access_path]
                
            logging.info('Issue tlog found. Going to fetch access log.')
                
            try:
                if self.dictionary_of_tlogs["CHARGE_TYPE"] == 'A':
                    access_log = subprocess.run(["grep", f"/subscription/ActivateSubscription?", f"{access_path}/localhost_access_log.{date_formated[0]}-{date_formated[1]}-{date_formated[2]}.txt"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
                    for data in access_log.stdout.splitlines():
                        if re.search(r"\b{}\b".format(str(msisdn)),data):
                            self.acc_log = f"{data}{self.new_line}"
                    
                elif self.dictionary_of_tlogs["CHARGE_TYPE"] == 'D':
                    access_log = subprocess.run(["grep", f"/subscription/DeactivateSubscription?", f"{access_path}/localhost_access_log.{date_formated[0]}-{date_formated[1]}-{date_formated[2]}.txt"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
                    for data in access_log.stdout.splitlines():
                        if re.search(r"\b{}\b".format(str(msisdn)),data):
                            self.acc_log = f"{data}{self.new_line}"
                        
                elif self.dictionary_of_tlogs["CHARGE_TYPE"] == 'E':
                    access_log = subprocess.run(["grep", f"/subscription/EventCharge?", f"{access_path}/localhost_access_log.{date_formated[0]}-{date_formated[1]}-{date_formated[2]}.txt"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
                    for data in access_log.stdout.splitlines():
                        if re.search(r"\b{}\b".format(str(msisdn)),data):
                            self.acc_log = f"{data}{self.new_line}"
                        
                elif self.dictionary_of_tlogs["CHARGE_TYPE"] == 'U':
                    access_log = subprocess.run(["grep", f"/subscription/UpgradeSubscription?", f"{access_path}/localhost_access_log.{date_formated[0]}-{date_formated[1]}-{date_formated[2]}.txt"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
                    for data in access_log.stdout.splitlines():
                        if re.search(r"\b{}\b".format(str(msisdn)),data):
                            self.acc_log = f"{data}{self.new_line}"
                    
                elif self.dictionary_of_tlogs["CHARGE_TYPE"] == 'T':
                    access_log = subprocess.run(["grep", f"/subscription/TriggerCharge?", f"{access_path}/localhost_access_log.{date_formated[0]}-{date_formated[1]}-{date_formated[2]}.txt"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
                    for data in access_log.stdout.splitlines():
                        if re.search(r"\b{}\b".format(str(msisdn)),data):
                            self.acc_log = f"{data}{self.new_line}"
                    
                elif self.dictionary_of_tlogs["CHARGE_TYPE"] == 'G':
                    access_log = subprocess.run(["grep", f"/subscription/ChargeGift?", f"{access_path}/localhost_access_log.{date_formated[0]}-{date_formated[1]}-{date_formated[2]}.txt"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
                    for data in access_log.stdout.splitlines():
                        if re.search(r"\b{}\b".format(str(msisdn)),data):
                            self.acc_log = f"{data}{self.new_line}"
                            
                elif self.dictionary_of_tlogs["CHARGE_TYPE"] == 'V':
                    access_log = subprocess.run(["grep", f"/subscription/AddRenewalTrigger?", f"{access_path}/localhost_access_log.{date_formated[0]}-{date_formated[1]}-{date_formated[2]}.txt"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
                    for data in access_log.stdout.splitlines():
                        if re.search(r"\b{}\b".format(str(msisdn)),data):
                            self.acc_log = f"{data}{self.new_line}"
                    
                with open(self.issue_tlog_path, "a") as write_file:
                    write_file.writelines(self.acc_log)
                        
            except subprocess.CalledProcessError as ex:
                try:
                    if self.dictionary_of_tlogs["CHARGE_TYPE"] == 'A':
                        access_log = subprocess.run(["grep", f"/subscription/ActivateSubscription?", f"{self.initializedPath_object.dict_of_process_dir['tomcat']['PROCESS_HOME_DIR']}/{access_path}/localhost_access_log.{date_formated[0]}-{date_formated[1]}-{date_formated[2]}.txt"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
                        for data in access_log.stdout.splitlines():
                            if re.search(r"\b{}\b".format(str(msisdn)),data):
                                self.acc_log = f"{data}{self.new_line}"
                                    
                    elif self.dictionary_of_tlogs["CHARGE_TYPE"] == 'D':
                        access_log = subprocess.run(["grep", f"/subscription/DeactivateSubscription?", f"{self.initializedPath_object.dict_of_process_dir['tomcat']['PROCESS_HOME_DIR']}/{access_path}/localhost_access_log.{date_formated[0]}-{date_formated[1]}-{date_formated[2]}.txt"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
                        for data in access_log.stdout.splitlines():
                            if re.search(r"\b{}\b".format(str(msisdn)),data):
                                self.acc_log = f"{data}{self.new_line}"
                            
                    elif self.dictionary_of_tlogs["CHARGE_TYPE"] == 'E':
                        access_log = subprocess.run(["grep", f"/subscription/EventCharge?", f"{access_path}/localhost_access_log.{date_formated[0]}-{date_formated[1]}-{date_formated[2]}.txt"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
                        for data in access_log.stdout.splitlines():
                            if re.search(r"\b{}\b".format(str(msisdn)),data):
                                self.acc_log = f"{data}{self.new_line}"
                        
                    elif self.dictionary_of_tlogs["CHARGE_TYPE"] == 'U':
                        access_log = subprocess.run(["grep", f"/subscription/UpgradeSubscription?", f"{access_path}/localhost_access_log.{date_formated[0]}-{date_formated[1]}-{date_formated[2]}.txt"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
                        for data in access_log.stdout.splitlines():
                            if re.search(r"\b{}\b".format(str(msisdn)),data):
                                self.acc_log = f"{data}{self.new_line}"
                        
                    elif self.dictionary_of_tlogs["CHARGE_TYPE"] == 'T':
                        access_log = subprocess.run(["grep", f"/subscription/TriggerCharge?", f"{access_path}/localhost_access_log.{date_formated[0]}-{date_formated[1]}-{date_formated[2]}.txt"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
                        for data in access_log.stdout.splitlines():
                            if re.search(r"\b{}\b".format(str(msisdn)),data):
                                self.acc_log = f"{data}{self.new_line}"
                        
                    elif self.dictionary_of_tlogs["CHARGE_TYPE"] == 'G':
                        access_log = subprocess.run(["grep", f"/subscription/ChargeGift?", f"{access_path}/localhost_access_log.{date_formated[0]}-{date_formated[1]}-{date_formated[2]}.txt"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
                        for data in access_log.stdout.splitlines():
                            if re.search(r"\b{}\b".format(str(msisdn)),data):
                                self.acc_log = f"{data}{self.new_line}"
                            
                    elif self.dictionary_of_tlogs["CHARGE_TYPE"] == 'V':
                        access_log = subprocess.run(["grep", f"/subscription/AddRenewalTrigger?", f"{access_path}/localhost_access_log.{date_formated[0]}-{date_formated[1]}-{date_formated[2]}.txt"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
                        for data in access_log.stdout.splitlines():
                            if re.search(r"\b{}\b".format(str(msisdn)),data):
                                self.acc_log = f"{data}{self.new_line}"
                        
                    logging.info('Access log found. Writing to a file : %s', self.issue_tlog_path)
                    with open(self.issue_tlog_path, "a") as write_file:
                        write_file.writelines(self.acc_log)
                except subprocess.CalledProcessError as ex:
                    logging.info('No access log found') 
                                    
            with open(self.issue_tlog_path, "a") as write_file:
                logging.info('Writing issue tlog to a file: %s', self.issue_tlog_path)
                self.issue_tlog_data_prism = tlogParser_object.filtered_prism_tlog[-1]
                write_file.writelines(self.issue_tlog_data_prism)
            
        
        elif tlogParser_object.filtered_tomcat_tlog and (self.is_error_tlog or self.is_lowbal_tlog or self.is_retry_tlog or self.is_nhf_tlog or self.is_await_push_tlog):
            
            access_path = self.initializedPath_object.tomcat_log_path_dict[self.initializedPath_object.tomcat_access_path]
            
            logging.info('Issue tlog found. Going to fetch access log.')
            try:
                if self.dictionary_of_tlogs["CHARGE_TYPE"] == 'A':
                    access_log = subprocess.run(["grep", f"/subscription/RealTimeActivate?", f"{access_path}/localhost_access_log.{date_formated[0]}-{date_formated[1]}-{date_formated[2]}.txt"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
                    for data in access_log.stdout.splitlines():
                        if re.search(r"\b{}\b".format(str(msisdn)),data):
                            self.acc_log = f"{data}{self.new_line}"
                
                elif self.dictionary_of_tlogs["CHARGE_TYPE"] == 'D':
                    access_log = subprocess.run(["grep", f"/subscription/RealTimeDeactivate?", f"{access_path}/localhost_access_log.{date_formated[0]}-{date_formated[1]}-{date_formated[2]}.txt"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
                    for data in access_log.stdout.splitlines():
                        if re.search(r"\b{}\b".format(str(msisdn)),data):
                            self.acc_log = f"{data}{self.new_line}"
                
                elif self.dictionary_of_tlogs["CHARGE_TYPE"] == 'E':
                    access_log = subprocess.run(["grep", f"/subscription/RealTimeCharge?", f"{access_path}/localhost_access_log.{date_formated[0]}-{date_formated[1]}-{date_formated[2]}.txt"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
                    for data in access_log.stdout.splitlines():
                        if re.search(r"\b{}\b".format(str(msisdn)),data):
                            self.acc_log = f"{data}{self.new_line}"
                
                elif self.dictionary_of_tlogs["CHARGE_TYPE"] == 'F':
                    access_log = subprocess.run(["grep", f"/subscription/RealTimeTransactionRefund?", f"{access_path}/localhost_access_log.{date_formated[0]}-{date_formated[1]}-{date_formated[2]}.txt"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
                    for data in access_log.stdout.splitlines():
                        if re.search(r"\b{}\b".format(str(msisdn)),data):
                            self.acc_log = f"{data}{self.new_line}"
                
                logging.info('Access log found is: %s', self.acc_log)
                with open(self.issue_tlog_path, "a") as write_file:
                    write_file.writelines(self.acc_log)
            
            except subprocess.CalledProcessError as ex:
                try: 
                    if self.dictionary_of_tlogs["CHARGE_TYPE"] == 'A':
                        access_log = subprocess.run(["grep", f"/subscription/RealTimeActivate?", f"{self.initializedPath_object.dict_of_process_dir['tomcat']['PROCESS_HOME_DIR']}/{access_path}/localhost_access_log.{date_formated[0]}-{date_formated[1]}-{date_formated[2]}.txt"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
                        for data in access_log.stdout.splitlines():
                            if re.search(r"\b{}\b".format(str(msisdn)),data):
                                self.acc_log = f"{data}{self.new_line}"
                        
                    elif self.dictionary_of_tlogs["CHARGE_TYPE"] == 'D':
                        access_log = subprocess.run(["grep", f"/subscription/RealTimeDeactivate?", f"{self.initializedPath_object.dict_of_process_dir['tomcat']['PROCESS_HOME_DIR']}/{access_path}/localhost_access_log.{date_formated[0]}-{date_formated[1]}-{date_formated[2]}.txt"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
                        for data in access_log.stdout.splitlines():
                            if re.search(r"\b{}\b".format(str(msisdn)),data):
                                self.acc_log = f"{data}{self.new_line}"
                    
                    elif self.dictionary_of_tlogs["CHARGE_TYPE"] == 'E':
                        access_log = subprocess.run(["grep", f"/subscription/RealTimeCharge?", f"{access_path}/localhost_access_log.{date_formated[0]}-{date_formated[1]}-{date_formated[2]}.txt"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
                        for data in access_log.stdout.splitlines():
                            if re.search(r"\b{}\b".format(str(msisdn)),data):
                                self.acc_log = f"{data}{self.new_line}"
                        
                    elif self.dictionary_of_tlogs["CHARGE_TYPE"] == 'F':
                        access_log = subprocess.run(["grep", f"/subscription/RealTimeTransactionRefund?", f"{access_path}/localhost_access_log.{date_formated[0]}-{date_formated[1]}-{date_formated[2]}.txt"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
                        for data in access_log.stdout.splitlines():
                            if re.search(r"\b{}\b".format(str(msisdn)),data):
                                self.acc_log = f"{data}{self.new_line}"
                    
                    logging.info('Access log found. Writing to a file: %s', self.issue_tlog_path)
                    with open(self.issue_tlog_path, "a") as write_file:
                            write_file.writelines(self.acc_log)
                        
                except subprocess.CalledProcessError as ex:
                    logging.debug('No access log found') 
                
            #charge schedule < now check
            with open(self.issue_tlog_path, "a") as write_file:
                self.issue_tlog_data_tomcat = tlogParser_object.filtered_tomcat_tlog[-1]
                data = str(self.issue_tlog_data_tomcat).split("|")
                tdata = str(data[-1]).split(",")
                if datetime.strptime(f"{tdata[-2]}", "%Y-%m-%d %H:%M:%S.%f") < datetime.now():
                    logging.info('charge schedule < now = true. hence skipping tomcat log processing')
                    self.is_prism_processing_required = True
                else:
                    self.is_prism_processing_required = False
                    write_file.writelines(self.issue_tlog_data_tomcat)
                    logging.info("tomcat tlog charge schedule is greater than now. Hence not going to check for prism tlog. Kindly ignore below logs.")
                
        else:
            logging.debug('No issue tlog found for given msisdn: %s', msisdn)
            logging.debug('Hence not fetching the daemon log.')
                    

        self.get_trimmed_thread_log()

    def get_trimmed_thread_log(self):
        """
        Get daemon log for the given thread
        """

        # task = ""
        if len(self.issue_tlog_data_tomcat) != 0 and self.is_prism_processing_required == False:
            
            if not self.is_await_push_tlog:
                logging.info('Going to fetch tomcat daemon log for the issue thread : %s', self.dictionary_of_search_value["THREAD"])
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
                                        self.is_issue_in_thread = True
                                        break
                    elif self.is_lowbal_tlog:
                        for status in TlogLowBalTag:
                            with open(daemonLog_object.tomcat_thread_outfile, "r") as read_file:
                                for i, line in enumerate(read_file):
                                    if re.search(r"\b{}\b".format(str(status.value)), line):
                                        self.set_initial_index(i)
                                        self.task = status.name
                                        self.is_issue_in_thread = True
                                        break
                                    
                    elif self.is_retry_tlog:
                        for status in TlogRetryTag:
                            with open(daemonLog_object.tomcat_thread_outfile, "r") as read_file:
                                for i, line in enumerate(read_file):
                                    if re.search(r"\b{}\b".format(str(status.value)), line):
                                        self.set_initial_index(i)
                                        self.task = status.name
                                        self.is_issue_in_thread = True
                                        break
                    
                    elif self.is_nhf_tlog:
                        for status in TlogNHFTag:
                            with open(daemonLog_object.tomcat_thread_outfile, "r") as read_file:
                                for i, line in enumerate(read_file):
                                    if re.search(r"\b{}\b".format(str(status.value)), line):
                                        self.set_initial_index(i)
                                        self.task = status.name
                                        self.is_issue_in_thread = True
                                        break
                    
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
                        for status in TlogErrorTag:
                            with open(daemonLog_object.prismd_thread_outfile, "r") as read_file:
                                for i, line in enumerate(read_file):
                                    if re.search(r"\b{}\b".format(str(status.value)), line):
                                        self.set_initial_index(i)
                                        self.task = status.name
                                        self.is_issue_in_thread = True
                                        break
                                    
                    elif self.is_lowbal_tlog:
                        for status in TlogLowBalTag:
                            with open(daemonLog_object.prismd_thread_outfile, "r") as read_file:
                                for i, line in enumerate(read_file):
                                    if re.search(r"\b{}\b".format(str(status.value)), line):
                                        self.set_initial_index(i)
                                        self.task = status.name
                                        self.is_issue_in_thread = True
                                        break
                                    
                    elif self.is_retry_tlog:
                        for status in TlogRetryTag:
                            with open(daemonLog_object.prismd_thread_outfile, "r") as read_file:
                                for i, line in enumerate(read_file):
                                    if re.search(r"\b{}\b".format(str(status.value)), line):
                                        self.set_initial_index(i)
                                        self.task = status.name
                                        self.is_issue_in_thread = True
                                        break
                    
                    elif self.is_nhf_tlog:
                        for status in TlogNHFTag:
                            with open(daemonLog_object.prismd_thread_outfile, "r") as read_file:
                                for i, line in enumerate(read_file):
                                    if re.search(r"\b{}\b".format(str(status.value)), line):
                                        self.set_initial_index(i)
                                        self.task = status.name
                                        self.is_issue_in_thread = True
                                        break
                    
                    elif self.is_await_push_tlog:
                        for status in TlogAwaitPushTag:
                            with open(daemonLog_object.prismd_thread_outfile, "r") as read_file:
                                for i, line in enumerate(read_file):
                                    if re.search(r"\b{}\b".format(str(status.value)), line):
                                        self.set_initial_index(i)
                                        self.task = status.name
                                        self.is_issue_in_thread = True
                                        break
                    
                    if self.is_issue_in_thread:
                                       
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
                        logging.debug('%s present without containing the issue tag.', self.dictionary_of_search_value["THREAD"])
                        
                else:
                    logging.debug("Prism daemon log doesn't exist for the issue thread %s : ", self.dictionary_of_search_value["THREAD"])
            else:
                logging.debug('Eigther transaction is in await push state or timed out.')
                logging.debug('Check for notification callback. Daemon log processing not required.')
                
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