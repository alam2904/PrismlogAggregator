import logging
from datetime import datetime
from outfile_writer import FileWriter

class AutoTlog:
    
    def __init__(self):
        self.tlog_record_list = []
    
    def parse_tlog_btw_timestamps(self, validation_object, tlog_data_automation_outfile, billing_tlog_files):
        
        start_date = datetime.strptime(validation_object.f_diff_date_time, "%Y-%m-%d %H:%M:%S")
        end_date = datetime.strptime(validation_object.f_cur_date_time, "%Y-%m-%d %H:%M:%S")
        for file in billing_tlog_files:
            with open(file, "r") as read_file:
                record = [data for data in read_file.readlines()]
                for data in record[2:]:
                    splited_data = data.split("|")
                    splited_timestamp = splited_data[0].split(",")
                    tlog_timestamp = datetime.strptime(splited_timestamp[0], "%Y-%m-%d %H:%M:%S")
                    if tlog_timestamp >= start_date and tlog_timestamp <= end_date:    
                        self.tlog_record_list.append(data)
        if self.tlog_record_list:
            writer = FileWriter()
            writer.write_automation_tlog_data(tlog_data_automation_outfile, self.tlog_record_list)
            return True
        else:
            return False