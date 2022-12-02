import logging
from datetime import datetime
from outfile_writer import FileWriter
import subprocess
import re
from configparser import ConfigParser
import signal
from tlog_tag import webapps
from pathlib import Path
from log_files import LogFileFinder

class Automation:
    
    def __init__(self):
        self.tlog_record_list = []
        self.alog_record_list = []
    
    def parse_tlog_btw_timestamps(self, validation_object, tlog_data_automation_outfile, billing_tlog_files):
        
        start_date = datetime.strptime(validation_object.f_diff_date_time, "%Y%m%d%H%M%S")
        end_date = datetime.strptime(validation_object.f_cur_date_time, "%Y%m%d%H%M%S")
        for record in billing_tlog_files:
            splited_data = record.split("|")
            splited_timestamp = splited_data[0].split(",")
            tlog_time = datetime.strptime(splited_timestamp[0], "%Y-%m-%d %H:%M:%S")
            tlog_timest = datetime.strftime(tlog_time, "%Y%m%d%H%M%S")
            tlog_timestamp = datetime.strptime(tlog_timest, "%Y%m%d%H%M%S")
            if tlog_timestamp >= start_date and tlog_timestamp <= end_date:  
                self.tlog_record_list.append(record)
        
        if self.tlog_record_list:
            writer = FileWriter()
            writer.write_automation_tlog_data(tlog_data_automation_outfile, self.tlog_record_list)
            return True
        else:
            return False
    
    def parse_alog_btw_timestamps(self, msisdn, validation_object, alog_data_automation_outfile, access_path):
        config = ConfigParser()
        file = Path('config.properties')
        new_line = '\n'
        if file.exists():
            config.read(file)
            start_date = datetime.strptime(validation_object.f_diff_date_time, "%Y%m%d%H%M%S")
            end_date = datetime.strptime(validation_object.f_cur_date_time, "%Y%m%d%H%M%S")

            logging.info('start date is: %s', start_date)
            logging.info('end date is: %s', end_date)
            alog_file = LogFileFinder(start_date, access_path)
            alog_file.access_log_files_automation(config, end_date)
            try:
                for file in alog_file.alog_files_list:
                    for servlet_mapping in webapps.servlet_mapping.value:
                        try:
                            access_log = subprocess.check_output(f"grep {servlet_mapping} {file}", universal_newlines=True, shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
                            for pos, record in enumerate(access_log.splitlines()):
                                for data in record.split("-"):
                                    if re.search(msisdn, data, re.DOTALL):
                                        splited_data = datetime.strptime(data.split('"')[0].split(" ")[1].split("[")[1], "%d/%b/%Y:%H:%M:%S")
                                        if splited_data >= start_date and splited_data <= end_date:
                                            self.alog_record_list.append(data)
                                            logging.info('access log for: %s found in: %s at line: %s', msisdn, file, pos)
                        except subprocess.CalledProcessError as err:
                            logging.info('access log for: %s count not be found in: %s further', msisdn, file)
                
                if self.alog_record_list:
                    writer = FileWriter()
                    writer.write_automation_alog_data(alog_data_automation_outfile, self.alog_record_list)
                            
            except ValueError as err:
                logging.info('eigther list empty or some error.')
            
            
            