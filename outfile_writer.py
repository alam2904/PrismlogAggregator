import json
import logging
import os
import shutil
import socket
from zipfile import ZipFile
import zipfile
# import subprocess

class FileWriter:
    
    def __init__(self, outputDirectory_object, oarm_uid):
        self.outputDirectory_object = outputDirectory_object
        self.oarm_uid = oarm_uid
        self.hostname = socket.gethostname()
        
    def write_json_tlog_data(self, prism_data_dict):
        #dumping payment transaction data
        with open("{0}/{1}_prismTransactionData.json".format(self.outputDirectory_object, self.hostname), "w") as outfile:
            json.dump(prism_data_dict, outfile, indent=4)
    
    def write_complete_tomcat_gs_thread_log(self, pname, folder, thread, record, http_error_code):
        #write complete thread log
        thread_outfile = ""
        logging.info("THIS METHOD CALLED: %s", thread)
        
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
        except FileNotFoundError as error:
            logging.info(error)
            
        if pname == "GENERIC_SERVER_REQ_RESP" or pname == "GENERIC_SERVER_REQ_RESP_GS":
            self.write_trimmed_thread_log(pname, folder, thread, thread_outfile, http_error_code, None, None, None)
        
                
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