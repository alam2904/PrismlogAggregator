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
    def __init__(self, msisdn, input_date, tlog_record_list_prism, tlog_record_list_tomcat, tlog_record_list_sms, initializedPath_object):
        self.msisdn = msisdn
        self.input_date = input_date
        self.tlog_record_list_prism = tlog_record_list_prism
        self.tlog_record_list_tomcat = tlog_record_list_tomcat
        self.tlog_record_list_sms = tlog_record_list_sms
        self.initializedPath_object = initializedPath_object
    
    def get_prism_billing_tlog(self):
        """
        calling path finder method
        """
        logfile_object = LogFileFinder(self.input_date, self.initializedPath_object)

        logfile_object.prism_billing_tlog_path()
        
        if logfile_object.is_prism_billing_tlog_path:
            tlog_file = logfile_object.prism_billing_tlog_files(self.input_date)
            if tlog_file != None:
                prism_billing_tlog_files = logfile_object.prism_billing_tlog_files(self.input_date)
                for file in prism_billing_tlog_files:
                    with open(file, "r") as read_file:
                        record = [data for data in read_file.readlines() if re.search(r"\b{}\b".format(str(self.msisdn)),data)]
        
                        if record:
                            for data in record:
                                self.tlog_record_list_prism.append(data)
                return True
            else:
                return False
        else:
            return False
    
    def get_tomcat_billing_tlog(self):
        """
        calling path finder method
        """
        logfile_object = LogFileFinder(self.input_date, self.initializedPath_object)

        logfile_object.tomcat_billing_tlog_path()
        
        if logfile_object.is_tomcat_billing_tlog_path:
            tlog_file = logfile_object.tomcat_billing_tlog_files(self.input_date)
            if tlog_file != None:
                tomcat_billing_tlog_files = list(logfile_object.tomcat_billing_tlog_files(self.input_date))
                for file in tomcat_billing_tlog_files:
                    with open(file, "r") as read_file:
                        # record = [data for data in read_file.readlines() if re.search(r"\b{}\b".format(str(self.msisdn)),data, re.DOTALL)]
                        record = [data for data in read_file.readlines() if re.search(self.msisdn,data, re.DOTALL)]
                                
                        if record:
                            for data in record:
                                self.tlog_record_list_tomcat.append(data)
                return True
            else:
                return False
        else:
            return False
    
    def get_sms_tlog(self):
        """
        calling path finder method
        """
        logfile_object = LogFileFinder(self.input_date, self.initializedPath_object)

        logfile_object.sms_tlog_path()
        
        if logfile_object.is_sms_tlog_path:
            tlog_file = logfile_object.sms_tlog_files(self.input_date)
            if tlog_file != None:
                sms_tlog_files = logfile_object.sms_tlog_files(self.input_date)
                for file in sms_tlog_files:
                    with open(file, "r") as read_file:
                        record = [data for data in read_file.readlines() if re.search(r"\b{}\b".format(str(self.msisdn)),data)]
        
                        if record:
                            for data in record:
                                self.tlog_record_list_sms.append(data)
                return True
            else:
                return False
        else:
            return False