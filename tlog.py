"""
tlog module
"""
import logging
import re
from log_files import LogFileFinder
from automation import Automation
from input_validation import InputValidation


class Tlog:
    """
    tlog get class
    """
    def __init__(self, msisdn, input_date, tlog_record_list_prism, tlog_record_list_tomcat, tlog_record_list_sms, plog_record_list_prism, plog_record_list_tomcat, initializedPath_object):
        self.msisdn = msisdn
        self.input_date = input_date
        self.tlog_record_list_prism = tlog_record_list_prism
        self.tlog_record_list_tomcat = tlog_record_list_tomcat
        self.tlog_record_list_sms = tlog_record_list_sms
        self.plog_record_list_prism = plog_record_list_prism
        self.plog_record_list_tomcat = plog_record_list_tomcat
        self.initializedPath_object = initializedPath_object
    
    def get_prism_billing_tlog_automation(self, validation_object, data_automation_outfile):
        """
        calling path finder method automation
        """
        logfile_object = LogFileFinder(self.input_date, self.initializedPath_object)
        # logfile_object = LogFileFinder(self.initializedPath_object)

        logfile_object.prism_billing_tlog_path()
        
        if logfile_object.is_prism_billing_tlog_path:
            auto_tlog = Automation()
            
            tlog_file = logfile_object.prism_billing_tlog_files_automation()
            if tlog_file != None:
                prism_billing_tlog_files = logfile_object.prism_billing_tlog_files_automation()
                for file in prism_billing_tlog_files:
                    with open(file, "r") as read_file:
                        record = [data for data in read_file.readlines() if re.search(self.msisdn,data, re.DOTALL)]
                        if record:
                            for data in record:
                                self.tlog_record_list_prism.append(data)
                
                is_auto = auto_tlog.parse_tlog_btw_timestamps(validation_object, data_automation_outfile, self.tlog_record_list_prism)
                if is_auto:
                    return is_auto
            else:
                return False
        else:
            return False
    
    def get_prism_perf_log_automation(self, validation_object, data_automation_outfile):
        """
        calling path finder method automation
        """
        logfile_object = LogFileFinder(self.input_date, self.initializedPath_object)
        # logfile_object = LogFileFinder(self.initializedPath_object)

        logfile_object.prism_billing_tlog_path()
        
        if logfile_object.is_prism_billing_tlog_path:
            auto_tlog = Automation()
            
            plog_file = logfile_object.prism_perf_log_files_automation()
            if plog_file != None:
                prism_billing_plog_files = logfile_object.prism_perf_log_files_automation()
                for file in prism_billing_plog_files:
                    with open(file, "r") as read_file:
                        record = [data for data in read_file.readlines() if re.search(self.msisdn,data, re.DOTALL)]
                        if record:
                            for data in record:
                                self.plog_record_list_prism.append(data)
                
                is_auto = auto_tlog.parse_plog_btw_timestamps(validation_object, data_automation_outfile, self.plog_record_list_prism)
                if is_auto:
                    return is_auto
            else:
                return False
        else:
            return False
    
    def get_prism_billing_tlog(self, validation_object):
        """
        calling path finder method
        """
        logfile_object = LogFileFinder(self.input_date, self.initializedPath_object)
        tlog_record = []

        logfile_object.prism_billing_tlog_path()
        
        if logfile_object.is_prism_billing_tlog_path:
            tlog_file = logfile_object.prism_billing_tlog_files(self.input_date)
            if tlog_file != None:
                prism_billing_tlog_files = logfile_object.prism_billing_tlog_files(self.input_date)
                for file in prism_billing_tlog_files:
                    with open(file, "r") as read_file:
                        record = [data for data in read_file.readlines() if re.search(self.msisdn,data, re.DOTALL)]
                        if record:
                            for data in record:
                                tlog_record.append(data)

                if tlog_record:
                    for data in tlog_record: 
                        if validation_object.service_keyword == str(data).split("|")[6]:
                            self.tlog_record_list_prism.append(data)
                return True
            else:
                return False
        else:
            return False
    
    def get_prism_perf_log(self):
        """
        calling path finder method
        """
        logfile_object = LogFileFinder(self.input_date, self.initializedPath_object)

        logfile_object.prism_perf_log_path()
        
        if logfile_object.is_prism_perf_log_path:
            plog_file = logfile_object.prism_perf_log_files(self.input_date)
            if plog_file != None:
                prism_billing_plog_files = logfile_object.prism_perf_log_files(self.input_date)
                for file in prism_billing_plog_files:
                    with open(file, "r") as read_file:
                        record = [data for data in read_file.readlines() if re.search(self.msisdn,data, re.DOTALL)]
                        if record:
                            for data in record:
                                self.plog_record_list_prism.append(data)
                return True
            else:
                return False
        else:
            return False
    
    def get_tomcat_billing_tlog_automation(self, validation_object, data_automation_outfile):
        """
        calling path finder method for automation
        """
        logfile_object = LogFileFinder(self.input_date, self.initializedPath_object)

        logfile_object.tomcat_billing_tlog_path()
        
        if logfile_object.is_tomcat_billing_tlog_path:
            auto_tlog = Automation()
            
            tlog_file = logfile_object.tomcat_billing_tlog_files_automation()
            if tlog_file != None:
                tomcat_billing_tlog_files = list(logfile_object.tomcat_billing_tlog_files_automation())
                for file in tomcat_billing_tlog_files:
                    with open(file, "r") as read_file:
                        record = [data for data in read_file.readlines() if re.search(self.msisdn,data, re.DOTALL)]
                        if record:
                            for data in record:
                                self.tlog_record_list_tomcat.append(data)
                
                is_auto = auto_tlog.parse_tlog_btw_timestamps(validation_object, data_automation_outfile, self.tlog_record_list_tomcat)
                if is_auto:
                    return is_auto
            else:
                return False
        else:
            return False
    
    def get_tomcat_perf_log_automation(self, validation_object, data_automation_outfile):
        """
        calling path finder method for automation
        """
        logfile_object = LogFileFinder(self.input_date, self.initializedPath_object)

        logfile_object.tomcat_perf_log_path()
        
        if logfile_object.is_tomcat_perf_log_path:
            auto_plog = Automation()    
            plog_file = logfile_object.tomcat_perf_log_files_automation()
            if plog_file != None:
                tomcat_perf_log_files = list(logfile_object.tomcat_perf_log_files_automation())
                for file in tomcat_perf_log_files:
                    with open(file, "r") as read_file:
                        record = [data for data in read_file.readlines() if re.search(self.msisdn,data, re.DOTALL)]
                        if record:
                            for data in record:
                                self.plog_record_list_tomcat.append(data)
                
                is_auto = auto_plog.parse_plog_btw_timestamps(validation_object, data_automation_outfile, self.plog_record_list_tomcat)
                if is_auto:
                    return is_auto
            else:
                return False
        else:
            return False
    
    def get_tomcat_perf_log(self):
        """
        calling path finder method
        """
        logfile_object = LogFileFinder(self.input_date, self.initializedPath_object)

        logfile_object.tomcat_perf_log_path()
        
        if logfile_object.is_tomcat_perf_log_path:
            plog_file = logfile_object.tomcat_perf_log_files(self.input_date)
            if plog_file != None:
                tomcat_billing_plog_files = list(logfile_object.tomcat_perf_log_files(self.input_date))
                for file in tomcat_billing_plog_files:
                    with open(file, "r") as read_file:
                        record = [data for data in read_file.readlines() if re.search(self.msisdn,data, re.DOTALL)]
                                    
                        if record:
                            for data in record:
                                self.plog_record_list_tomcat.append(data)
                return True
            else:
                return False
        else:
            return False
    
    def get_tomcat_billing_tlog(self, validation_object):
        """
        calling path finder method
        """
        tlog_record = []
        logfile_object = LogFileFinder(self.input_date, self.initializedPath_object)

        logfile_object.tomcat_billing_tlog_path()
        
        if logfile_object.is_tomcat_billing_tlog_path:
            tlog_file = logfile_object.tomcat_billing_tlog_files(self.input_date)
            if tlog_file != None:
                tomcat_billing_tlog_files = list(logfile_object.tomcat_billing_tlog_files(self.input_date))
                for file in tomcat_billing_tlog_files:
                    with open(file, "r") as read_file:
                        record = [data for data in read_file.readlines() if re.search(self.msisdn,data, re.DOTALL)]    
                        if record:
                            for data in record:
                                tlog_record.append(data)
                
                if tlog_record:
                    for data in tlog_record:
                        if validation_object.service_keyword == str(data).split("|")[6]:
                            self.tlog_record_list_tomcat.append(data)
                        
                return True
            else:
                return False
        else:
            return False
    
    def get_sms_tlog(self, validation_object):
        """
        calling path finder method
        """
        tlog_record = []
        logfile_object = LogFileFinder(self.input_date, self.initializedPath_object)

        logfile_object.sms_tlog_path()
        
        if logfile_object.is_sms_tlog_path:
            tlog_file = logfile_object.sms_tlog_files(self.input_date)
            if tlog_file != None:
                sms_tlog_files = logfile_object.sms_tlog_files(self.input_date)
                for file in sms_tlog_files:
                    with open(file, "r") as read_file:
                        record = [data for data in read_file.readlines() if re.search(self.msisdn,data, re.DOTALL)]
                        if record:
                            for data in record:
                                tlog_record.append(data)
                
                if tlog_record:
                    for data in tlog_record:
                        if validation_object.service_keyword == str(data).split("|")[6]:
                            self.tlog_record_list_sms.append(data)
                        
                return True
            else:
                return False
        else:
            return False