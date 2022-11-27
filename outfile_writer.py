import logging
import os

class FileWriter:
        
    def write_access_log(self, issue_tlog_path, acc_log):
        logging.info('Access log found. Writing to a file : %s', issue_tlog_path)
        try:
            with open(issue_tlog_path, "r") as write_file:
                for line in write_file:
                    if acc_log == line:
                        logging.info('access log exists in tlog out file.')
                        break
                else:
                    with open(issue_tlog_path, "a") as write_file:
                        logging.info('appending')
                        write_file.writelines(acc_log)
        except FileNotFoundError as ex:
            with open(issue_tlog_path, "a") as write_file:
                write_file.writelines(acc_log)
            
    def write_issue_tlog(self, issue_tlog_path, issue_tlog_data):
        logging.info('Writing issue tlog to a file: %s', issue_tlog_path)
        try:   
            with open(issue_tlog_path, "r") as write_file:
                for line in write_file:
                    if issue_tlog_data == line:
                        logging.info('tlog exists in tlog out file.')
                        break
                else:
                    with open(issue_tlog_path, "a") as write_file:
                        logging.info('appending')
                        write_file.writelines(issue_tlog_data)
        except FileNotFoundError as ex:
            try:
                with open(issue_tlog_path, "a") as write_file:
                    logging.info('file not found. appending')
                    write_file.writelines(issue_tlog_data)
            except FileNotFoundError as error:
                logging.info('file not found to write.')
            
    def write_complete_thread_log(self, record, thread_outfile):
        if os.path.isfile(thread_outfile) and os.path.getsize(thread_outfile) != 0:
            os.remove(thread_outfile)
        
        try:    
            with open(thread_outfile, "a") as write_file:
                write_file.writelines(record)
        except FileNotFoundError as error:
            logging.info('file not found to write.')
            
    def write_trimmed_thread_log(self, thread_outfile, trimmed_thread_outfile, initial_index, final_index):
        logging.info('thread file-%s, trimmed thread file-%s, initial index-%s, final index-%s', thread_outfile,trimmed_thread_outfile,initial_index,final_index)
        
        if os.path.isfile(trimmed_thread_outfile) and os.path.getsize(trimmed_thread_outfile) != 0:
            os.remove(trimmed_thread_outfile)
            
        with open(thread_outfile, "r") as read_file:
            for i, line in enumerate(read_file):
                if final_index <= i < initial_index + 1:
                    with open(trimmed_thread_outfile, "a") as write_file:
                        write_file.writelines(line)
        return True
    
    def write_automation_tlog_data(self, tlog_data_automation_outfile, tlog_record_list):    
        data = [data for data in tlog_record_list]
        with open(tlog_data_automation_outfile, "a") as write_file:
            write_file.writelines(data)
        
        
            
    #charge schedule < now check
            # with open(self.issue_tlog_path, "a") as write_file:
            #     logging.info('Writing issue tlog to a file: %s', self.issue_tlog_path)
                # data = str(self.issue_tlog_data_tomcat).split("|")
                # tdata = str(data[-1]).split(",")
                # if datetime.strptime(f"{tdata[-2]}", "%Y-%m-%d %H:%M:%S.%f") < datetime.now():
                #     logging.info('charge schedule < now = true. hence skipping tomcat log processing')
                #     self.is_prism_processing_required = True
                # else:
                #     self.is_prism_processing_required = False
                # write_file.writelines(self.issue_tlog_data_tomcat)
                    # logging.info("tomcat tlog charge schedule is greater than now. Hence not going to check for prism tlog. Kindly ignore below logs.")
    