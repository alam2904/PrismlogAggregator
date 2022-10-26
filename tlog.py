"""
tlog module
"""
import logging
import re
from log_files import LogFileFinder

class Tlog:
    """
    tlog get class
    """
    def __init__(self, msisdn, input_date, tlog_record_list):
        self.msisdn = msisdn
        self.input_date = input_date
        self.tlog_record_list = tlog_record_list
    
    def get_tlog(self):
        """
        calling path finder method
        """
        files_path = LogFileFinder()
        if files_path.prism_tlog_files(self.input_date) != None:
            logging.debug('Prism tlog path exists')
            prism_billing_tlog_files = list(files_path.prism_tlog_files(self.input_date))
            
            # if prism_billing_tlog_files:
            for file in prism_billing_tlog_files:
                with open(file, "r") as read_file:
                    record = [data for data in read_file.readlines() if re.search(r"\b{}\b".format(str(self.msisdn)),data)]
                        
                    if record:
                        self.tlog_record_list.append(record)
            return True
        else:
            return False