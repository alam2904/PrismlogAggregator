import json
import logging
import os
import re
import shutil
import socket
from zipfile import ZipFile
import zipfile
from input_tags import Prism_St_SString, Prism_En_SString
# import subprocess

class FileWriter:
    
    def __init__(self, outputDirectory_object, oarm_uid):
        self.outputDirectory_object = outputDirectory_object
        self.oarm_uid = oarm_uid
        self.hostname = socket.gethostname()
        self.__initial_index = 0
        self.__final_index = 0 
        
    def write_json_tlog_data(self, payment_data_dict):
                
        #dumping payment transaction data

        with open("{0}/{1}_prismTransactionData.json".format(self.outputDirectory_object, self.hostname), "w") as outfile:
            json.dump(payment_data_dict, outfile, indent=4)
    
    def write_complete_thread_log(self, pname, tlog_thread, record, ctid, task_type, sub_type, input_tag):
        #write complete thread log
        thread_outfile = ""
        process_folder = ""
        error_code = tlog_thread
        RequestOrigin = task_type
        
        if pname == "PRISM_TOMCAT":
            process_folder = os.path.join(self.outputDirectory_object, "{}_issue_prism_tomcat".format(self.hostname))                
            thread_outfile = "{0}/{1}_{2}_prism_tomcat.log".format(process_folder, task_type, tlog_thread)
        
        elif pname == "PRISM_DEAMON":
            process_folder = os.path.join(self.outputDirectory_object, "{}_issue_prism_daemon".format(self.hostname))      
            thread_outfile = "{0}/{1}_{2}_prism_daemon.log".format(process_folder, task_type, tlog_thread)
        
        elif pname == "PRISM_SMSD":
            process_folder = os.path.join(self.outputDirectory_object, "{}_issue_prism_smsd".format(self.hostname))                
            thread_outfile = "{0}/{1}_prism_smsd.log".format(process_folder, tlog_thread)
            
        try:
            with open(thread_outfile, "w") as write_file:
                write_file.writelines(record)
                if pname == "PRISM_TOMCAT" or pname == "PRISM_DEAMON":
                    self.write_trimmed_thread_log(pname, process_folder, tlog_thread, thread_outfile, ctid, task_type, sub_type, input_tag)
                    
        except FileNotFoundError as error:
            logging.info(error)
                
    def write_trimmed_thread_log(self, pname, process_folder, tlog_thread, thread_outfile, ctid, task_type, sub_type, input_tag):
        error_code = tlog_thread
        RequestOrigin = task_type
        trimmed_thread_outfile = ""
        
        if pname == "PRISM_TOMCAT":
            trimmed_thread_outfile = "{0}/{1}_{2}_trimmed_prism_tomcat.log".format(process_folder, task_type, tlog_thread)
        
        elif pname == "PRISM_DEAMON":
            trimmed_thread_outfile = "{0}/{1}_{2}_trimmed_prism_daemon.log".format(process_folder, task_type, tlog_thread)
        
        try:   
            if pname == "PRISM_TOMCAT" or pname == "PRISM_DEAMON":
                #set initial index based on start of search string
                for sm_start_serach_string_name, sm_start_serach_string_value in Prism_St_SString.__dict__.items():
                    if not sm_start_serach_string_name.startswith("__"):
                        # logging.info('st search: %s', str(sm_start_serach_string_value).format(task_type, sub_type))
                        sm_start_serach_string = str(sm_start_serach_string_value).format(task_type, sub_type)
                        with open(thread_outfile, "r") as outFile:
                            for i, line in enumerate(outFile):
                                if re.search(sm_start_serach_string, line, re.DOTALL):
                                    self.set_initial_index(i)
                                    break
                
                #set final index based on end of search string
                for sm_end_serach_string_name, sm_end_serach_string_value in Prism_En_SString.__dict__.items():
                    if not sm_end_serach_string_name.startswith("__"):
                        # logging.info('en search: %s', str(sm_end_serach_string_value).format(input_tag))
                        sm_end_serach_string = str(sm_end_serach_string_value).format(input_tag)
                        with open(thread_outfile, "r") as outFile:
                            for i, line in enumerate(outFile):
                                if re.search(sm_end_serach_string, line, re.DOTALL):
                                    self.set_final_index(i)
                                    break
                

        except FileNotFoundError as error:
            logging.info(error)
            
        logging.info('initial_index: %s and final_index: %s', self.__initial_index, self.__final_index)
        #write trim log
        if self.__initial_index != self.__final_index != 0:
            with open(thread_outfile, "r") as read_file:
                for i, line in enumerate(read_file):
                    if self.__initial_index <= i < self.__final_index + 1:
                        with open(trimmed_thread_outfile, "a") as write_file:
                            write_file.writelines(line)

    
    def set_initial_index(self, initial_index):
        """
        Setting initial index from
        """
        self.__initial_index = initial_index
    
    def get_initial_index(self):
        """
        getting initial index from
        """
        return self.__initial_index
    
    def set_final_index(self, final_index):
        """
        Setting initial index from
        """
        self.__final_index = final_index
    
    def get_final_index(self):
        """
        getting initial index from
        """
        return self.__final_index
                
    def zipped_outfile(self):
        #zipping the out folder
        out_zipFile = "{}_{}_outfile.zip".format(self.oarm_uid, self.hostname)
        
        with ZipFile(out_zipFile, "a", compression=zipfile.ZIP_DEFLATED) as zip:
            for root, dirs, files in os.walk(self.outputDirectory_object):
                for file in files:
                    zip.write(os.path.join(root, file))
        print("OARM_OUTPUT_FILENAME|{}".format(os.path.abspath(out_zipFile)))
        
        
    def log_mover(self):
        #move log_aggregator.log from current directory to respective directory.
        log = "{0}/{1}_aggregator.log".format(self.outputDirectory_object, self.hostname)
        
        if os.path.exists('aggregator.log'):
            try:
                if os.path.isfile(log):
                    logging.info('aggregator.log file already exists. Hence removing and copying it.')
                    os.remove(log)
                shutil.move('aggregator.log', log)
            except Exception as error:
                logging.info(error)