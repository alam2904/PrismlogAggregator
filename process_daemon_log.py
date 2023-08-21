from datetime import datetime, timedelta
import gzip
import logging
import os
import re
import shutil
import signal
import socket
import subprocess
from outfile_writer import FileWriter

class DaemonLogProcessor:
    """
        Daemon log processor class
        fetching daemon log for the issue threads in the tlogs
    """
    def __init__(self, initializedPath_object, outputDirectory_object, validation_object, oarm_uid):
        self.initializedPath_object = initializedPath_object
        self.outputDirectory_object = outputDirectory_object
        self.validation_object = validation_object
        self.oarm_uid = oarm_uid
        
        self.log_files = []
        self.csv_files = []
        self.backup_log_files = []
        
        self.start_date = validation_object.start_date
        self.end_date = validation_object.end_date
        
        self.input_date = []
        self.is_msisdn_backup_file = False
        self.is_backup_file = False
        self.is_backup_root_file = False
        
        self.s_date = datetime.strptime(datetime.strftime(self.start_date, "%Y%m%d"), "%Y%m%d")
        self.e_date = datetime.strptime(datetime.strftime(self.end_date, "%Y%m%d"), "%Y%m%d")
        self.issue_record = ""
        self.is_trimmed_log = False
        
        self.hostname = socket.gethostname()
        self.onmopay_out_folder = False
    
    def process_daemon_log_init(self, pname, tlog_thread, ctid, task_types, sub_type, input_tag):
        #msisdn log processing
        if pname == "PRISM_TOMCAT" or pname == "PRISM_DEAMON":
            logging.info("INPUT_TAGS: %s", input_tag)
            self.process_daemon_log(pname, tlog_thread, ctid, task_types, sub_type, input_tag)
        elif pname == "PRISM_SMSD":
            self.process_daemon_log(pname, tlog_thread, ctid, task_types, sub_type, input_tag)
        
        if not self.is_trimmed_log:
            return False
        return True  
        
    def process_daemon_log(self, pname, tlog_thread, ctid, task_types, sub_type, input_tag):
        #creating out file writter object for writting log to out file
        fileWriter_object = FileWriter(self.outputDirectory_object, self.oarm_uid)
        #msisdn based processing initially
        if pname == "PRISM_TOMCAT" or pname == "PRISM_DEAMON" or pname == "PRISM_SMSD":
            try:
                self.reinitialize_constructor_parameter()
                
                if pname == "PRISM_TOMCAT":
                    self.log_files.append(self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_TEST_{}_log".format(self.validation_object.fmsisdn)])
                elif pname == "PRISM_DEAMON":
                    self.log_files.append(self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_TEST_{}_log".format(self.validation_object.fmsisdn)])
                
                self.fetch_daemon_log(tlog_thread, self.log_files) 
                    
                if self.issue_record:
                    self.is_trimmed_log = fileWriter_object.write_complete_thread_log(pname, tlog_thread, self.issue_record, None, task_types, sub_type, input_tag)
            except KeyError as error:
                logging.error(error)
            
            #queue id 99 processing
            try:
                if not self.issue_record:
                    self.reinitialize_constructor_parameter()
                        
                    if pname == "PRISM_TOMCAT":
                        self.log_files.append(self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_PROCESSOR_99_log"])
                    elif pname == "PRISM_DEAMON":
                        self.log_files.append(self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_PROCESSOR_99_log"])
                    elif pname == "PRISM_SMSD":
                        self.log_files.append(self.initializedPath_object.prism_smsd_log_path_dict["prism_smsd_PROCESSOR_99_log"])
                        
                    self.fetch_daemon_log(tlog_thread, self.log_files) 
                    
                    if self.issue_record:
                        self.is_trimmed_log = fileWriter_object.write_complete_thread_log(pname, tlog_thread, self.issue_record, None, task_types, sub_type, input_tag)
            except KeyError as error:
                logging.info(error)
            
            #prism/tomcat log processing
            try:
                if not self.issue_record:
                    self.reinitialize_constructor_parameter()
                    
                    if pname == "PRISM_TOMCAT":
                        self.log_files.append(self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_PRISM_log"])
                    elif pname == "PRISM_DEAMON":
                        self.log_files.append(self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_PRISM_log"])
                    elif pname == "PRISM_SMSD":
                        self.log_files.append(self.initializedPath_object.prism_smsd_log_path_dict["prism_smsd_PRISM_log"])
                    
                    self.fetch_daemon_log(tlog_thread, self.log_files)
                    
                    if self.issue_record:
                        self.is_trimmed_log = fileWriter_object.write_complete_thread_log(pname, tlog_thread, self.issue_record, None, task_types, sub_type, input_tag)
            except KeyError as error:
                logging.info(error)
            
            #prism/tomcat root log processing
            try:
                if not self.issue_record:
                    self.reinitialize_constructor_parameter()
                    
                    if pname == "PRISM_TOMCAT":
                        self.log_files.append(self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_ROOT_log"])
                    elif pname == "PRISM_DEAMON":
                        self.log_files.append(self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_ROOT_log"])
                    elif pname == "PRISM_SMSD":
                        self.log_files.append(self.initializedPath_object.prism_smsd_log_path_dict["prism_smsd_ROOT_log"])
                    
                    self.fetch_daemon_log(tlog_thread, self.log_files) 
                    
                    if self.issue_record:
                        self.is_trimmed_log = fileWriter_object.write_complete_thread_log(pname, tlog_thread, self.issue_record, None, task_types, sub_type, input_tag)
            except KeyError as error:
                logging.info(error)
            
            #prism/tomcat log backup dated file processing
            try:
                if not self.issue_record:
                    
                    self.reinitialize_constructor_parameter()
                    self.is_backup_file = True
                    self.dated_log_files(pname)
                    self.fetch_daemon_log(tlog_thread, self.backup_log_files) 
                    
                    if self.issue_record:
                        self.is_trimmed_log = fileWriter_object.write_complete_thread_log(pname, tlog_thread, self.issue_record, None, task_types, sub_type, input_tag)
            except KeyError as error:
                logging.info(error)
            
            #prism/tomcat root log backup dated file processing
            try:
                if not self.issue_record:
                    
                    self.reinitialize_constructor_parameter()
                    self.is_backup_root_file = True
                    self.dated_log_files(pname)
                    self.fetch_daemon_log(tlog_thread, self.backup_log_files) 
                    
                    if self.issue_record:
                        self.is_trimmed_log = fileWriter_object.write_complete_thread_log(pname, tlog_thread, self.issue_record, None, task_types, sub_type, input_tag)
            except KeyError as error:
                logging.info(error)
    
    def dated_log_files(self, pname):
        try:            
            #method call to date range list
            self.input_date = self.date_range_list(self.s_date, self.e_date)
            
            for date in self.input_date:
                input_date_formatted = datetime.strftime(date, "%d")
                # logging.info('backup date: %s', input_date_formatted)
                input_date_formatted_month = datetime.strftime(date, "%Y-%m")

                if self.is_backup_file:
                    if pname == "PRISM_TOMCAT" or pname == "GENERIC_SERVER" or pname == "GENERIC_SERVER_REQ_RESP":
                        self.backup_log_files.append(str(self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_PRISM_backup_log"]).replace("yyyy-MM", "{}".format(input_date_formatted_month)).replace("dd", "{}".format(input_date_formatted)))
                    elif pname == "GENERIC_SERVER_REQ_RESP_GS":
                        self.backup_log_files.append(str(self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_GENERIC_SERVER_LOGGER_backup_log"]).replace("yyyy-MM", "{}".format(input_date_formatted_month)).replace("dd", "{}".format(input_date_formatted)))
                        
                    elif pname == "PRISM_DEAMON":
                        self.backup_log_files.append(self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_PRISM_backup_log"].replace("yyyy-MM", "{}".format(input_date_formatted_month)).replace("dd", "{}".format(input_date_formatted)))
                    
                    elif pname == "PRISM_SMSD":
                        self.backup_log_files.append(self.initializedPath_object.prism_smsd_log_path_dict["prism_smsd_PRISM_backup_log"].replace("yyyy-MM", "{}".format(input_date_formatted_month)).replace("dd", "{}".format(input_date_formatted)))
                
                elif self.is_backup_root_file:
                    if pname == "PRISM_TOMCAT" or pname == "GENERIC_SERVER":
                        self.backup_log_files.append(str(self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_ROOT_backup_log"]).replace("yyyy-MM", "{}".format(input_date_formatted_month)).replace("dd", "{}".format(input_date_formatted)))

                    elif pname == "PRISM_DEAMON":
                        self.backup_log_files.append(self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_ROOT_backup_log"].replace("yyyy-MM", "{}".format(input_date_formatted_month)).replace("dd", "{}".format(input_date_formatted)))
                    
                    elif pname == "PRISM_SMSD":
                        self.backup_log_files.append(self.initializedPath_object.prism_smsd_log_path_dict["prism_smsd_ROOT_backup_log"].replace("yyyy-MM", "{}".format(input_date_formatted_month)).replace("dd", "{}".format(input_date_formatted)))
                
        except KeyError as error:
            logging.info(error)
        
    def fetch_daemon_log(self, tlog_thread, log_files, date_formatted=None):
        #check file for the record for the given thread
        lines = []
        start_line = None
        end_line = None
        
        if self.is_msisdn_backup_file or self.is_backup_file or self.is_backup_root_file:
            for file in log_files:
                try:   
                    bk_files = subprocess.check_output("ls {0}".format(file), shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
                    bk_file_names = bk_files.splitlines()
                    logging.info("BK_FILE_NAMES: %s", bk_file_names)
                    for bkfile in bk_file_names:
                        try:
                            # completed_process = subprocess.run("zcat {0} | grep -l {1}".format(bkfile, tlog_thread), shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
                            if date_formatted:
                                completed_process = subprocess.Popen("zcat {0} | grep -a {} | grep -l {1}".format(bkfile, date_formatted, tlog_thread), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                            else:
                                completed_process = subprocess.Popen("zcat {0} | grep -l {1}".format(bkfile, tlog_thread), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                            
                            output, error = completed_process.communicate()
                            returncode = completed_process.returncode
                            
                            if returncode == 0:
                                output = output.strip()
                                if output == "(standard input)":
                                    logging.info('BACKUP_LOG_FILE: %s', bkfile)
                                    if date_formatted:
                                        with gzip.open(bkfile, 'rt') as file:
                                            for line_number, line in enumerate(file, start=1):
                                                if date_formatted in line and tlog_thread in line:
                                                    # logging.info('MATCHED_BACKUP_LOG_FILE: %s', bkfile)
                                                    if start_line is None:
                                                        start_line = line_number
                                                    end_line = line_number
                                                    lines.append(line)
                                                elif start_line is not None and not re.match(r'\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}\]-', line):
                                                    lines.append(line)
                                        if lines:
                                            logging.info("INNER BREAK")
                                            break
                                    else:
                                        with gzip.open(bkfile, 'rt') as file:
                                            for line_number, line in enumerate(file, start=1):
                                                if tlog_thread in line:
                                                    # logging.info('MATCHED_BACKUP_LOG_FILE: %s', bkfile)
                                                    if start_line is None:
                                                        start_line = line_number
                                                    end_line = line_number
                                                    lines.append(line)
                                                elif start_line is not None and not re.match(r'\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}\]-', line):
                                                    lines.append(line)
                                        if lines:
                                            logging.info("INNER BREAK")
                                            break
                                        
                                else:
                                    logging.info("Thread not found in: %s", bkfile)
                            else:
                                logging.info("returnCode returned non-zero")
                            
                        except subprocess.CalledProcessError as e:
                            # An error occurred while running the command
                            logging.info("Error: %s", e)
                    if lines:
                        logging.info("OUTER BREAK")
                        break
                except subprocess.CalledProcessError as e:
                    logging.debug("Error executing ls command: %s", e)
                except Exception as ex:
                    logging.debug(ex)
        else:
            for file in log_files:
                try:
                    if date_formatted:
                        with open(file, 'r') as file:
                            for line_number, line in enumerate(file, start=1):
                                if date_formatted in line and tlog_thread in line:
                                    if start_line is None:
                                        start_line = line_number
                                    end_line = line_number
                                    lines.append(line)
                                elif start_line is not None and not re.match(r'\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}\]-', line):
                                    lines.append(line)
                    else:
                        with open(file, 'r') as file:
                            for line_number, line in enumerate(file, start=1):
                                if tlog_thread in line:
                                    if start_line is None:
                                        start_line = line_number
                                    end_line = line_number
                                    lines.append(line)
                                elif start_line is not None and not re.match(r'\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}\]-', line):
                                    lines.append(line)
                except Exception as ex:
                    logging.debug(ex)
                
        logging.info("START_LINE: %s and END_LINE: %s", start_line, end_line)
        if lines:
            self.issue_record = lines
            
    def process_tomcat_http_req_resp_log(self, pname, folder, data_dict, issue_access_thread):
        #creating out file writter object for writting log to out file
        fileWriter_object = FileWriter(self.outputDirectory_object, self.oarm_uid)
        self.reinitialize_constructor_parameter()
        
        for thread in issue_access_thread:
            if not pname == "GENERIC_SERVER_REQ_RESP" and not pname == "GENERIC_SERVER_REQ_RESP_GS":
                data_thread = data_dict["THREAD"]
                date_formatted = self.thread_timestamp_formatting(data_dict["TIMESTAMP"])
                status_code = data_dict["HTTP_STATUS_CODE"]
            else:
                for item in data_dict:
                    data_thread = item["THREAD_ID"]
                    status_code = item["STATUS"]
                date_formatted = None
                
            if thread == data_thread:
                logging.info('thread: %s and formatted thread timestamp: %s', thread, date_formatted)
                try:
                    if not self.issue_record:
                        self.reinitialize_constructor_parameter()
                        if pname == "PRISM_TOMCAT" or pname == "GENERIC_SERVER" or \
                            pname == "GENERIC_SERVER_REQ_RESP" or pname == "GENERIC_SERVER_REQ_RESP_GS":
                            
                            if not pname == "GENERIC_SERVER_REQ_RESP_GS":
                                self.log_files.append(self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_PRISM_log"])                        
                            else:
                                self.log_files.append(self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_GENERIC_SERVER_LOGGER_log"])

                            logging.info("LOG_FILES: %s", self.log_files)
                            self.fetch_daemon_log(thread, self.log_files, date_formatted)
                                
                        if self.issue_record:
                            # logging.info("ISSUE_RECORD: %s", self.issue_record)
                            fileWriter_object.write_complete_tomcat_gs_thread_log(pname, folder, thread, self.issue_record, status_code)    
                except KeyError as error:
                    logging.info(error)
                
                #prism tomcat root log processing
                try:
                    if not self.issue_record:
                        self.reinitialize_constructor_parameter()
                        if pname == "PRISM_TOMCAT" or pname == "GENERIC_SERVER" or pname == "GENERIC_SERVER_REQ_RESP":
                            self.log_files.append(self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_ROOT_log"])                        
                            # self.fetch_tomcat_access_daemon_log(thread, date_formatted, self.log_files)
                            self.fetch_daemon_log(thread, self.log_files, date_formatted)
                                
                        if self.issue_record:
                            fileWriter_object.write_complete_tomcat_gs_thread_log(pname, folder, thread, self.issue_record, status_code)
                        
                except KeyError as error:
                    logging.info(error)
                    
                #prism/tomcat log backup dated file processing
                try:
                    if not self.issue_record:
                        self.reinitialize_constructor_parameter()
                        self.is_backup_file = True
                        self.dated_log_files(pname)
                        # self.fetch_tomcat_access_daemon_log(thread, date_formatted, self.backup_log_files)
                        self.fetch_daemon_log(thread, self.backup_log_files, date_formatted)
                        
                        if self.issue_record:
                            fileWriter_object.write_complete_tomcat_gs_thread_log(pname, folder, thread, self.issue_record, status_code)
                        
                except KeyError as error:
                    logging.info(error)
                
                #prism/tomcat root log backup dated file processing
                try:
                    if not self.issue_record:
                        
                        self.reinitialize_constructor_parameter()
                        self.is_backup_root_file = True
                        self.dated_log_files(pname)
                        # self.fetch_tomcat_access_daemon_log(thread, date_formatted, self.backup_log_files)
                        self.fetch_daemon_log(thread, self.backup_log_files, date_formatted)
                        
                        if self.issue_record:
                            fileWriter_object.write_complete_tomcat_gs_thread_log(pname, folder, thread, self.issue_record, status_code)
                            
                except KeyError as error:
                    logging.info(error)
    
    def thread_timestamp_formatting(self, thread_timestamp):
        #date formatter
        date_format = "%d/%b/%Y:%H:%M:%S"
        date_formatted = datetime.strftime(datetime.strptime(thread_timestamp, date_format), "%Y-%m-%d %H:%M:%S")
        return date_formatted
    
    def date_range_list(self, start_date, end_date):
        # Return list of datetime.date objects between start_date and end_date (inclusive).
        date_list = []
        curr_date = start_date
        while curr_date <= end_date:
            date_list.append(curr_date)
            curr_date += timedelta(days=1)
        return date_list
    
    def create_process_folder(self, pname, folder):
        """
            creating process folder
        """
        if os.path.exists(folder):
            # delete the existing folder
            shutil.rmtree(folder)

        # create a new folder
        os.makedirs(folder)
            
    def set_process_out_folder(self, is_true):
        self.onmopay_out_folder = is_true
            
    def reinitialize_constructor_parameter(self):
        self.input_date = []
        self.log_files = []
        self.backup_log_files = []
        self.issue_record = ""
        self.is_msisdn_backup_file = False
        self.is_backup_file = False
        self.is_backup_root_file = False
        self.is_trimmed_log = False
        
        