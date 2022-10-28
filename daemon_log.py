"""
importing required modules
"""
# import re
import logging
import subprocess
from subprocess import PIPE
from pathlib import Path
from log_files import LogFileFinder

class DaemonLog:
    """
    daemon log get class
    """
    def __init__(self, input_date, worker_log_recod_list, worker_thread, initializedPath_object):
        self.input_date = input_date
        self.worker_log_recod_list = worker_log_recod_list
        self.worker_thread = worker_thread
        self.initializedPath_object = initializedPath_object
        self.target = Path()/"out.txt"
    
    def get_prism_log(self):
        """
        calling path finder method
        """
        logPath_object = LogFileFinder(self.input_date, self.initializedPath_object)
        try:
            self.find_prism_log(logPath_object, logPath_object.prism_daemonlog_file())
            return True
        except subprocess.CalledProcessError as ex:
            logging.error('Prism daemon log path does not exists. Going to check root log.')

            try:
                self.find_prism_log(logPath_object, logPath_object.prism_rootlog_file())
                return True
            except subprocess.CalledProcessError as ex:
                logging.error('Prism root log path does not exists. Going to check prism backup log path')

                try:
                    self.find_prism_log(logPath_object, logPath_object.prism_daemonlog_backup_file())
                    return True
                except subprocess.CalledProcessError as ex:
                    logging.error('Prism backup log path does not exists. Going to check root backup log path')

                    try:
                        self.find_prism_log(logPath_object, logPath_object.prism_rootlog_backup_file())
                    except subprocess.CalledProcessError as ex:
                        logging.error('Prism root backup log path does not exists.')
                        logging.error(ex)
                        return False

        # try:
        #     worker_thread_log = subprocess.run(["grep", f"{self.worker_thread}", f"{logPath_object.prism_daemonlog_file()}"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
        #     record = [data for data in worker_thread_log.stdout]
        #     with open(self.target, "a") as write_file:
        #         write_file.writelines(record)
        #     return True
        # except subprocess.CalledProcessError as ex:
        #     logging.error('Prism daemon log path does not exists. Going to check root log.')

        #     try:
        #         worker_thread_log = subprocess.run(["grep", f"{self.worker_thread}", f"{logPath_object.prism_rootlog_file()}"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
        #         record = [data for data in worker_thread_log.stdout]
        #         with open(self.target, "a") as write_file:
        #             write_file.writelines(record)
        #         return True
        #     except subprocess.CalledProcessError as ex:
                # logging.error('Prism root log path does not exists. Going to check prism backup log path')
                # logging.error(ex)
                # return False
    
    def find_prism_log(self, logPath_object, logPath):
        try:
            worker_thread_log = subprocess.run(["grep", f"{self.worker_thread}", f"{logPath}"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
            record = [data for data in worker_thread_log.stdout]
            with open(self.target, "a") as write_file:
                write_file.writelines(record)
        except subprocess.CalledProcessError as ex:
            raise
            


                # with open(prism_billing_daemonlog_file, "r") as read_file:
                #     record = [data for data in read_file.readlines() if re.search(r"\b{}\b".format(str(self.worker_thread)),data)]
                        
                #     if record:
                #         with open(self.target, "a") as write_file:
                #             write_file.writelines(record)  
                # return True
        # else:
        #     return False         