"""
importing required modules
"""
import re
import logging
from pathlib import Path
from log_files import LogFileFinder

class DaemonLog:
    """
    daemon log get class
    """
    def __init__(self, worker_log_recod_list, worker_thread):
        self.worker_log_recod_list = worker_log_recod_list
        self.worker_thread = worker_thread
        self.target = Path()/"out.txt"
    
    def get_prism_log(self):
        """
        calling path finder method
        """
        files_path = LogFileFinder()
        if files_path.prism_daemonlog_files() != None:
            logging.debug('Prism daemon log path exists')
            prism_billing_daemonlog_file = files_path.prism_daemonlog_files()
            
            if prism_billing_daemonlog_file:
                with open(prism_billing_daemonlog_file, "r") as read_file:
                    record = [data for data in read_file.readlines() if re.search(r"\b{}\b".format(str(self.worker_thread)),data)]
                        
                    if record:
                        with open(self.target, "a") as write_file:
                            write_file.writelines(record)  
            return True
        else:
            return False         