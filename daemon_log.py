"""
importing required modules
"""
# import re
import logging
import subprocess
from subprocess import PIPE
from sys import stderr
from pathlib import Path
from log_files import LogFileFinder

class DaemonLog:
    """
    daemon log get class
    """
    def __init__(self, worker_log_recod_list, worker_thread, initializedPath_object):
        self.worker_log_recod_list = worker_log_recod_list
        self.worker_thread = worker_thread
        self.initializedPath_object = initializedPath_object
        self.target = Path()/"out.txt"
    
    def get_prism_log(self):
        """
        calling path finder method
        """
        logPath_object = LogFileFinder(self.initializedPath_object)
        if logPath_object.prism_daemonlog_files() != None:
            logging.debug('Prism daemon log path exists')
            prism_billing_daemonlog_file = logPath_object.prism_daemonlog_files()
            logging.debug('d log path %s : ', prism_billing_daemonlog_file)
            
            if prism_billing_daemonlog_file:
                try:
                    worker_thread_log = subprocess.run(["grep", f"{self.worker_thread}", f"{prism_billing_daemonlog_file}"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
                    record = [data for data in worker_thread_log.stdout]
                    with open(self.target, "a") as write_file:
                        write_file.writelines(record)
                    return True
                except subprocess.CalledProcessError as ex:
                    logging.error(ex)
                    return False
                # with open(prism_billing_daemonlog_file, "r") as read_file:
                #     record = [data for data in read_file.readlines() if re.search(r"\b{}\b".format(str(self.worker_thread)),data)]
                        
                #     if record:
                #         with open(self.target, "a") as write_file:
                #             write_file.writelines(record)  
                # return True
        else:
            return False         