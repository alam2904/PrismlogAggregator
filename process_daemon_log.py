from datetime import datetime, timedelta
import logging
import os
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
        
        self.hostname = socket.gethostname()
        self.onmopay_out_folder = False
        
    def process_daemon_log(self, pname, tlog_thread, ctid, task_type, sub_type, input_tag):
        #creating out file writter object for writting log to out file
        fileWriter_object = FileWriter(self.outputDirectory_object, self.oarm_uid)
        
        if pname == "PRISM_TOMCAT" or pname == "PRISM_DEAMON" or pname == "PRISM_SMSD":
            #msisdn log processing
            try:
                self.reinitialize_constructor_parameter()
                
                if pname == "PRISM_TOMCAT":
                    self.log_files.append(self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_TEST_{}_log".format(self.validation_object.fmsisdn)])
                elif pname == "PRISM_DEAMON":
                    self.log_files.append(self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_TEST_{}_log".format(self.validation_object.fmsisdn)])
                
                self.fetch_daemon_log(tlog_thread, self.log_files) 
                    
                if self.issue_record:
                    fileWriter_object.write_complete_thread_log(pname, tlog_thread, self.issue_record, None, task_type, sub_type, input_tag)
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
                        fileWriter_object.write_complete_thread_log(pname, tlog_thread, self.issue_record, None, task_type, sub_type, input_tag)
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
                        fileWriter_object.write_complete_thread_log(pname, tlog_thread, self.issue_record, None, task_type, sub_type, input_tag)
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
                        fileWriter_object.write_complete_thread_log(pname, tlog_thread, self.issue_record, None, task_type, sub_type, input_tag)
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
                        fileWriter_object.write_complete_thread_log(pname, tlog_thread, self.issue_record, None, task_type, sub_type, input_tag)
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
                        fileWriter_object.write_complete_thread_log(pname, tlog_thread, self.issue_record, None, task_type, sub_type, input_tag)
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
                    if pname == "PRISM_TOMCAT":
                        self.backup_log_files.append(str(self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_PRISM_backup_log"]).replace("yyyy-MM", "{}".format(input_date_formatted_month)).replace("dd", "{}".format(input_date_formatted)))

                    elif pname == "PRISM_DEAMON":
                        self.backup_log_files.append(self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_PRISM_backup_log"].replace("yyyy-MM", "{}".format(input_date_formatted_month)).replace("dd", "{}".format(input_date_formatted)))
                    
                    elif pname == "PRISM_SMSD":
                        self.backup_log_files.append(self.initializedPath_object.prism_smsd_log_path_dict["prism_smsd_PRISM_backup_log"].replace("yyyy-MM", "{}".format(input_date_formatted_month)).replace("dd", "{}".format(input_date_formatted)))
                
                elif self.is_backup_root_file:
                    if pname == "PRISM_TOMCAT":
                        self.backup_log_files.append(str(self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_ROOT_backup_log"]).replace("yyyy-MM", "{}".format(input_date_formatted_month)).replace("dd", "{}".format(input_date_formatted)))

                    elif pname == "PRISM_DEAMON":
                        self.backup_log_files.append(self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_ROOT_backup_log"].replace("yyyy-MM", "{}".format(input_date_formatted_month)).replace("dd", "{}".format(input_date_formatted)))
                    
                    elif pname == "PRISM_SMSD":
                        self.backup_log_files.append(self.initializedPath_object.prism_smsd_log_path_dict["prism_smsd_ROOT_backup_log"].replace("yyyy-MM", "{}".format(input_date_formatted_month)).replace("dd", "{}".format(input_date_formatted)))
                
        except KeyError as error:
            logging.info(error)
        
    def fetch_daemon_log(self, tlog_thread, log_files):
        #check file for the recod for the given thread
        for file in log_files:
            try:
                # logging.info('tlog thread is: %s and log_file is: %s', tlog_thread, file)
                
                if self.is_msisdn_backup_file or self.is_backup_file or self.is_backup_root_file:
                    thread_log = subprocess.check_output("zcat {0} | grep -a {1}".format(file, tlog_thread), shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
                else:
                    thread_log = subprocess.check_output("grep -a {0} {1}".format(tlog_thread, file), shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
                
                record = [data for data in thread_log]
                if record:
                    self.issue_record = record
            except subprocess.CalledProcessError as error:
                logging.info('eigther %s does not exists or %s could not be found', file, tlog_thread)
                logging.info(error)
    
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
        
        