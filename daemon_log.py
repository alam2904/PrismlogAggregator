"""
importing required modules
"""
from doctest import OutputChecker
import logging
import subprocess
from subprocess import PIPE
import signal
from log_files import LogFileFinder
from outfile_writer import FileWriter

class DaemonLog:
    """
    daemon log get class
    """
    def __init__(self, msisdn, input_date, worker_log_recod_list, worker_thread, initializedPath_object, tomcat_thread_outfile, prismd_thread_outfile, smsd_thread_outfile):
        self.msisdn = msisdn
        self.input_date = input_date
        self.worker_log_recod_list = worker_log_recod_list
        self.worker_thread = worker_thread
        self.initializedPath_object = initializedPath_object
        self.is_backup_path = False
        
        self.tomcat_thread_outfile = tomcat_thread_outfile
        self.prismd_thread_outfile = prismd_thread_outfile
        self.smsd_thread_outfile = smsd_thread_outfile

    
    def get_tomcat_log(self):
        """
        calling path finder method
        """
        logPath_object = LogFileFinder(self.input_date, self.initializedPath_object)
        try:
            is_tqlog = self.find_tomcat_log(logPath_object.tomcat_queue_id_99_log_file(), self.is_backup_path)
            if is_tqlog:
                logging.debug('Issue thread [%s] found in tomcat queue_id_99 log and will be parsed for any issue.', self.worker_thread)
                return is_tqlog
        except subprocess.CalledProcessError as ex:
            logging.warning('Tomcat queue_id_99 log path does not exists or issue thread [%s] could not be found.', self.worker_thread) 
            logging.debug('Going to check tomcat log.')
            try:
                is_tdlog = self.find_tomcat_log(logPath_object.tomcat_daemonlog_file(), self.is_backup_path)
                if is_tdlog:
                    logging.debug('Issue thread [%s] found in tomcat daemon log and will be parsed for any issue.', self.worker_thread)
                    return is_tdlog
            except subprocess.CalledProcessError as ex:
                logging.warning('Tomcat daemon log path does not exists or issue thread [%s] could not be found.',self.worker_thread) 
                logging.debug('Going to check root log.')

                try:
                    is_trlog = self.find_tomcat_log(logPath_object.tomcat_rootlog_file(), self.is_backup_path)
                    if is_trlog:
                        logging.debug('Issue thread [%s] found in tomcat root log and will be parsed for any issue.', self.worker_thread)
                        return is_trlog

                except subprocess.CalledProcessError as ex:
                    logging.warning('Tomcat root log path does not exists or issue thread [%s] could not be found.', self.worker_thread)
                    logging.debug('Going to check tomcat backup log path')

                    try:
                        self.is_backup_path = True
                        is_tdblog = self.find_tomcat_log(logPath_object.tomcat_daemonlog_backup_file(), self.is_backup_path)
                        if is_tdblog:
                            logging.debug('Issue thread [%s] found in tomcat daemon backup log and will be parsed for any issue.', self.worker_thread)
                            return is_tdblog

                    except subprocess.CalledProcessError as ex:
                        logging.warning('Tomcat backup log path does not exists or issue thread [%s] could not be found.', self.worker_thread)
                        logging.debug('Going to check root backup log path')

                        try:
                            self.is_backup_path = True
                            is_trblog = self.find_tomcat_log(logPath_object.tomcat_rootlog_backup_file(), self.is_backup_path)
                            if is_trblog:
                                logging.debug('Issue thread [%s] found in tomcat root backup log and will be parsed for any issue.', self.worker_thread)
                                return is_trblog
                        except subprocess.CalledProcessError as ex:
                            logging.warning('Tomcat root backup log path does not exists or issue thread [%s] could not be found.', self.worker_thread)
                            logging.error(ex)
                            logging.error('No tomcat logs could be found against %s or log may not be in debug mode', self.worker_thread)
                            return False
    
    def get_prism_log(self):
        """
        calling path finder method
        """
        logPath_object = LogFileFinder(self.input_date, self.initializedPath_object)
        try:
            is_pqlog = self.find_prism_log(logPath_object.prism_queue_id_99_log_file(), self.is_backup_path)
            if is_pqlog:
                logging.debug('Issue thread [%s] found in prism queue_id_99 log and will be parsed for any issue.', self.worker_thread)
                return is_pqlog
        except subprocess.CalledProcessError as ex:
            logging.warning('Prism queue_id_99 log path does not exists or issue thread [%s] could not be found.',self.worker_thread)
            logging.debug('Going to check prismd log.')
            try:
                is_pdlog = self.find_prism_log(logPath_object.prism_daemonlog_file(), self.is_backup_path)
                if is_pdlog:
                    logging.debug('Issue thread [%s] found in prism daemon log and will be parsed for any issue.', self.worker_thread)
                    return is_pdlog
            except subprocess.CalledProcessError as ex:
                logging.warning('Prism daemon log path does not exists or issue thread [%s] could not be found.',self.worker_thread) 
                logging.debug('Going to check root log.')

                try:
                    is_prlog = self.find_prism_log(logPath_object.prism_rootlog_file(), self.is_backup_path)
                    if is_prlog:
                        logging.debug('Issue thread [%s] found in prism root log and will be parsed for any issue.', self.worker_thread)
                        return is_prlog

                except subprocess.CalledProcessError as ex:
                    logging.warning('Prism root log path does not exists or issue thread [%s] could not be found.', self.worker_thread)
                    logging.debug('Going to check prism backup log path')

                    try:
                        self.is_backup_path = True
                        is_pdblog = self.find_prism_log(logPath_object.prism_daemonlog_backup_file(), self.is_backup_path)
                        if is_pdblog:
                            logging.debug('Issue thread [%s] found in prism daemon backup log and will be parsed for any issue.', self.worker_thread)
                            return is_pdblog

                    except subprocess.CalledProcessError as ex:
                        logging.warning('Prism backup log path does not exists or issue thread [%s] could not be found.', self.worker_thread)
                        logging.debug('Going to check root backup log path')

                        try:
                            self.is_backup_path = True
                            is_prblog = self.find_prism_log(logPath_object.prism_rootlog_backup_file(), self.is_backup_path)
                            if is_prblog:
                                logging.debug('Issue thread [%s] found in prism root backup log and will be parsed for any issue.', self.worker_thread)
                                return is_prblog
                        except subprocess.CalledProcessError as ex:
                            logging.warning('Prism root backup log path does not exists or issue thread [%s] could not be found.',self.worker_thread)
                            logging.error(ex)
                            logging.error('no prism logs could be found against %s or log may not be in debug mode', self.worker_thread)
                            return False
    
    def get_sms_log(self):
        """
        calling path finder method
        """
        logPath_object = LogFileFinder(self.input_date, self.initializedPath_object)
        try:
            is_sqlog = self.find_sms_log(logPath_object.sms_queue_id_99_log_file(), self.is_backup_path)
            if is_sqlog:
                logging.debug('Issue thread [%s] found in sms queue_id_99 log and will be parsed for any issue.', self.worker_thread)
                return is_sqlog
        except subprocess.CalledProcessError as ex:
            logging.warning('Sms queue_id_99 log path does not exists or issue thread [%s] could not be found.',self.worker_thread)
            logging.debug('Going to check smsd log.')
            try:
                is_sdlog = self.find_sms_log(logPath_object.sms_daemonlog_file(), self.is_backup_path)
                if is_sdlog:
                    logging.debug('Issue thread [%s] found in sms daemon log and will be parsed for any issue.', self.worker_thread)
                    return is_sdlog
            except subprocess.CalledProcessError as ex:
                logging.warning('Sms daemon log path does not exists or issue thread [%s] could not be found.',self.worker_thread) 
                logging.debug('Going to check root log.')

                try:
                    is_srlog = self.find_sms_log(logPath_object.sms_rootlog_file(), self.is_backup_path)
                    if is_srlog:
                        logging.debug('Issue thread [%s] found in sms root log and will be parsed for any issue.', self.worker_thread)
                        return is_srlog

                except subprocess.CalledProcessError as ex:
                    logging.warning('Sms root log path does not exists or issue thread [%s] could not be found.', self.worker_thread)
                    logging.debug('Going to check sms backup log path')

                    try:
                        self.is_backup_path = True
                        is_sdblog = self.find_sms_log(logPath_object.sms_daemonlog_backup_file(), self.is_backup_path)
                        if is_sdblog:
                            logging.debug('Issue thread [%s] found in sms daemon backup log and will be parsed for any issue.', self.worker_thread)
                            return is_sdblog

                    except subprocess.CalledProcessError as ex:
                        logging.warning('Sms backup log path does not exists or issue thread [%s] could not be found.', self.worker_thread)
                        logging.debug('Going to check root backup log path')

                        try:
                            self.is_backup_path = True
                            is_srblog = self.find_sms_log(logPath_object.sms_rootlog_backup_file(), self.is_backup_path)
                            if is_srblog:
                                logging.debug('Issue thread [%s] found in sms root backup log and will be parsed for any issue.', self.worker_thread)
                                return is_srblog
                        except subprocess.CalledProcessError as ex:
                            logging.warning('Sms root backup log path does not exists or issue thread [%s] could not be found.',self.worker_thread)
                            logging.error(ex)
                            logging.error('no sms logs could be found against %s or log may not be in debug mode', self.worker_thread)
                            return False
    
    def find_tomcat_log(self, logPath, is_backup_path):
        if not logPath is None:
            log_writer = FileWriter()
            try:
                if is_backup_path:
                    worker_thread_log = subprocess.check_output(f"zgrep -a {self.worker_thread} {logPath}", universal_newlines=True, shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
                    record = [data for data in worker_thread_log]

                else:
                    worker_thread_log = subprocess.check_output(f"grep -a {self.worker_thread} {logPath}", universal_newlines=True, shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
                    record = [data for data in worker_thread_log]
                
                log_writer.write_complete_thread_log(record, self.tomcat_thread_outfile)
                return True
            except subprocess.CalledProcessError as ex:
                raise
        else:
            return False
        
    def find_prism_log(self, logPath, is_backup_path):
        if not logPath is None:
            log_writer = FileWriter()
            try:
                if is_backup_path:
                    worker_thread_log = subprocess.check_output(f"zgrep -a {self.worker_thread} {logPath}", universal_newlines=True, shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
                    record = [data for data in worker_thread_log]

                else:
                    worker_thread_log = subprocess.check_output(f"grep -a {self.worker_thread} {logPath}", universal_newlines=True, shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
                    record = [data for data in worker_thread_log]
                
                log_writer.write_complete_thread_log(record, self.prismd_thread_outfile)
                return True
            except subprocess.CalledProcessError as ex:
                raise
        else:
            return False
    
    def find_sms_log(self, logPath, is_backup_path):
        if not logPath is None:
            log_writer = FileWriter()
            try:
                if is_backup_path:
                    worker_thread_log = subprocess.check_output(f"zgrep -a {self.worker_thread} {logPath}", universal_newlines=True, shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
                    record = [data for data in worker_thread_log]
                        
                else:
                    worker_thread_log = subprocess.check_output(f"grep -a {self.worker_thread} {logPath}", universal_newlines=True, shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
                    record = [data for data in worker_thread_log]
                
                log_writer.write_complete_thread_log(record, self.smsd_thread_outfile)
                return True
            except subprocess.CalledProcessError as ex:
                raise
        else:
            return False