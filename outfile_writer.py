import json
import logging
import os
import re
import shutil
import socket
from zipfile import ZipFile
import zipfile
from status_tags import Prism_St_SString, Prism_En_SString
# import subprocess

class FileWriter:
    
    def __init__(self, outputDirectory_object, oarm_uid):
        self.outputDirectory_object = outputDirectory_object
        self.oarm_uid = oarm_uid
        self.hostname = socket.gethostname()
        self.__initial_index = 0
        self.__final_index = 0
        self.is_trimmed_log = False
        
    def write_json_tlog_data(self, payment_data_dict):
        #dumping payment transaction data
        with open("{0}/{1}_prismTransactionData.json".format(self.outputDirectory_object, self.hostname), "w") as outfile:
            json.dump(payment_data_dict, outfile, indent=4)
    
    def write_complete_thread_log(self, pname, tlog_thread, record, ctid, task_types, sub_type, input_tag=None):
        #write complete thread log
        thread_outfile = ""
        process_folder = ""
        error_code = tlog_thread
        RequestOrigin = task_types
        logging.info("INPUT_TAG_OUTFILE: %s", input_tag)
        
        if pname == "PRISM_TOMCAT":
            process_folder = os.path.join(self.outputDirectory_object, "{}_issue_prism_tomcat".format(self.hostname))
            thread_outfile = "{0}/{1}_prism_tomcat.log".format(process_folder, tlog_thread)
        
        elif pname == "PRISM_DEAMON":
            process_folder = os.path.join(self.outputDirectory_object, "{}_issue_prism_daemon".format(self.hostname))      
            thread_outfile = "{0}/{1}_prism_daemon.log".format(process_folder, tlog_thread)
        
        elif pname == "PRISM_SMSD":
            process_folder = os.path.join(self.outputDirectory_object, "{}_issue_prism_smsd".format(self.hostname))                
            thread_outfile = "{0}/{1}_prism_smsd.log".format(process_folder, tlog_thread)
            
        try:
            with open(thread_outfile, "w") as write_file:
                write_file.writelines(record)
                if pname == "PRISM_TOMCAT" or pname == "PRISM_DEAMON":
                    return self.write_trimmed_thread_log(pname, process_folder, tlog_thread, thread_outfile, ctid, task_types, sub_type, input_tag)
                    
        except FileNotFoundError as error:
            logging.info(error)
    
    def write_complete_access_thread_log(self, pname, folder, thread, record, http_error_code):
        #write complete thread log
        thread_outfile = ""
        
        if pname == "PRISM_TOMCAT":           
            thread_outfile = "{0}/{1}_{2}_prism_access_tomcat.log".format(folder, http_error_code, thread)
        elif pname == "GENERIC_SERVER":
            thread_outfile = "{0}/{1}_{2}_generic_server_access.log".format(folder, http_error_code, thread)
        elif pname == "GENERIC_SERVER_REQ_RESP":
            thread_outfile = "{0}/{1}_{2}_generic_server_tomcat.log".format(folder, http_error_code, thread)
        elif pname == "GENERIC_SERVER_REQ_RESP_GS":
            thread_outfile = "{0}/{1}_{2}_generic_server.log".format(folder, http_error_code, thread)
        
        try:
            with open(thread_outfile, "w") as write_file:
                write_file.writelines(record)
                # if pname == "PRISM_TOMCAT" or pname == "PRISM_DEAMON":
                #     # self.write_trimmed_thread_log(pname, process_folder, tlog_thread, thread_outfile, ctid, task_type, sub_type, input_tag)
                #     pass
        except FileNotFoundError as error:
            logging.info(error)
        
    def write_trimmed_thread_log(self, pname, process_folder, tlog_thread, thread_outfile, ctid, task_types, sub_type, input_tag):
        
        error_code = tlog_thread
        RequestOrigin = task_types
        trimmed_thread_outfile = ""
        index = 0
        
        for task_type in task_types:
            self.reinitialize_index()
            if pname == "PRISM_TOMCAT":
                trimmed_thread_outfile = "{0}/{1}_{2}_trimmed_prism_tomcat.log".format(process_folder, task_type, tlog_thread)
            
            elif pname == "PRISM_DEAMON":
                trimmed_thread_outfile = "{0}/{1}_{2}_trimmed_prism_daemon.log".format(process_folder, task_type, tlog_thread)
            
            try:
                if pname == "PRISM_TOMCAT" or pname == "PRISM_DEAMON":
                    #set initial index based on start of search string
                    for sm_start_serach_string_name, sm_start_serach_string_value in Prism_St_SString.__dict__.items():
                        if not sm_start_serach_string_name.startswith("__"):
                            for ssString in sm_start_serach_string_value:
                                sm_start_serach_string = str(ssString).format(task_type, sub_type)
                                with open(thread_outfile, "r") as outFile:
                                    for i, line in enumerate(outFile):
                                        if re.search(sm_start_serach_string, line, re.DOTALL):
                                            self.set_initial_index(i)
                                            break
                    
                    #set final index based on end of search string
                    for sm_end_serach_string_name, sm_end_serach_string_value in Prism_En_SString.__dict__.items():
                        if not sm_end_serach_string_name.startswith("__"):
                            for esString in sm_end_serach_string_value:
                                sm_end_serach_string = str(esString).format(input_tag[index])
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
                self.is_trimmed_log = True
            index += 1
        
        return self.is_trimmed_log

    def write_handler_files(self, handler_files, macro_name_list, folder):
        # Execute the shell command to copy the file
        try:
            for file in handler_files:
                if file:
                    filename = os.path.basename(file)
                    destination_file = os.path.join(folder, filename)
                    shutil.copy2(file, destination_file)
                    # shutil.copy2(file, folder)
        except IOError as error:
            logging.error(error)
        
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
        
    def reinitialize_index(self):
        self.__initial_index = 0
        self.__final_index = 0
        
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