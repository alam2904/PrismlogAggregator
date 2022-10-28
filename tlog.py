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
    def __init__(self, msisdn, input_date, tlog_record_list, initializedPath_object):
        self.msisdn = msisdn
        self.input_date = input_date
        self.tlog_record_list = tlog_record_list
        self.initializedPath_object = initializedPath_object
    
    def get_prism_billing_tlog(self):
        """
        calling path finder method
        """
        logfile_object = LogFileFinder(self.initializedPath_object)

        logfile_object.prism_billing_tlog_path()
        
        if logfile_object.is_prism_billing_tlog_path:
            tlog_file = logfile_object.prism_billing_tlog_files(self.input_date)
            if tlog_file != None:
                prism_billing_tlog_files = list(logfile_object.prism_billing_tlog_files(self.input_date))
                for file in prism_billing_tlog_files:
                    with open(file, "r") as read_file:
                        record = [data for data in read_file.readlines() if re.search(r"\b{}\b".format(str(self.msisdn)),data)]
                                
                        if record:
                            self.tlog_record_list.append(record)
                return True
            else:
                return False
        else:
            return False