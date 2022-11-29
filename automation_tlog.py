import logging
from datetime import datetime
from outfile_writer import FileWriter
import subprocess

class AutoTlog:
    
    def __init__(self):
        self.tlog_record_list = []
    
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