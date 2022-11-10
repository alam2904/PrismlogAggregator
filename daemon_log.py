"""
importing required modules
"""
import logging
import subprocess
from subprocess import PIPE
import signal
from log_files import LogFileFinder

class DaemonLog:
    """
    daemon log get class
    """
    def __init__(self, input_date, worker_log_recod_list, worker_thread, initializedPath_object, outputDirectory_object):
        self.input_date = input_date
        self.worker_log_recod_list = worker_log_recod_list
        self.worker_thread = worker_thread
        self.initializedPath_object = initializedPath_object
        self.is_backup_path = False
        self.tomcat_thread_outfile = outputDirectory_object/"tomcat.log"
        self.prismd_thread_outfile = outputDirectory_object/"prismd.log"
    
    def get_tomcat_log(self):
        """
        calling path finder method
        """
        logPath_object = LogFileFinder(self.input_date, self.initializedPath_object)
        try:
            self.find_tomcat_log(logPath_object.tomcat_daemonlog_file(), self.is_backup_path)
            logging.debug('Issue thread [%s] found in tomcat daemon log and will be parsed for any issue.', self.worker_thread)
            return True
        except subprocess.CalledProcessError as ex:
            logging.warning('Tomcat daemon log path does not exists or issue thread [%s] could not be found.',self.worker_thread) 
            logging.debug('Going to check root log.')

            try:
                self.find_tomcat_log(logPath_object.tomcat_rootlog_file(), self.is_backup_path)
                logging.debug('Issue thread [%s] found in tomcat root log and will be parsed for any issue.', self.worker_thread)
                return True

            except subprocess.CalledProcessError as ex:
                logging.warning('Tomcat root log path does not exists or issue thread [%s] could not be found.', self.worker_thread)
                logging.debug('Going to check tomcat backup log path')

                try:
                    self.is_backup_path = True
                    self.find_tomcat_log(logPath_object.tomcat_daemonlog_backup_file(), self.is_backup_path)
                    logging.debug('Issue thread [%s] found in tomcat daemon backup log and will be parsed for any issue.', self.worker_thread)
                    return True

                except subprocess.CalledProcessError as ex:
                    logging.warning('Tomcat backup log path does not exists or issue thread [%s] could not be found.', self.worker_thread)
                    logging.debug('Going to check root backup log path')

                    try:
                        self.is_backup_path = True
                        self.find_tomcat_log(logPath_object.tomcat_rootlog_backup_file(), self.is_backup_path)
                        logging.debug('Issue thread [%s] found in tomcat root backup log and will be parsed for any issue.', self.worker_thread)
                        return True
                    except subprocess.CalledProcessError as ex:
                        logging.warning('Tomcat root backup log path does not exists or issue thread [%s] could not be found.', self.worker_thread)
                        logging.debug('Going to check tomcat queue_id_99 log path')
                        try:
                            self.find_tomcat_log(logPath_object.tomcat_queue_id_99_log_file(), self.is_backup_path)
                            logging.debug('Issue thread [%s] found in tomcat queue_id_99 log and will be parsed for any issue.', self.worker_thread)
                            return True
                        except subprocess.CalledProcessError as ex:
                            logging.warning('Tomcat queue_id_99 log path does not exists or issue thread [%s] could not be found.', self.worker_thread)
                            logging.error(ex)
                            logging.error('No tomcat logs could be found against %s or log may not be in debug mode', self.worker_thread)
                            return False
    
    def get_prism_log(self):
        """
        calling path finder method
        """
        logPath_object = LogFileFinder(self.input_date, self.initializedPath_object)
        try:
            self.find_prism_log(logPath_object.prism_daemonlog_file(), self.is_backup_path)
            logging.debug('Issue thread [%s] found in prism daemon log and will be parsed for any issue.', self.worker_thread)
            return True
        except subprocess.CalledProcessError as ex:
            logging.warning('Prism daemon log path does not exists or issue thread [%s] could not be found.',self.worker_thread) 
            logging.debug('Going to check root log.')

            try:
                self.find_prism_log(logPath_object.prism_rootlog_file(), self.is_backup_path)
                logging.debug('Issue thread [%s] found in prism root log and will be parsed for any issue.', self.worker_thread)
                return True

            except subprocess.CalledProcessError as ex:
                logging.warning('Prism root log path does not exists or issue thread [%s] could not be found.', self.worker_thread)
                logging.debug('Going to check prism backup log path')

                try:
                    self.is_backup_path = True
                    self.find_prism_log(logPath_object.prism_daemonlog_backup_file(), self.is_backup_path)
                    logging.debug('Issue thread [%s] found in prism daemon backup log and will be parsed for any issue.', self.worker_thread)
                    return True

                except subprocess.CalledProcessError as ex:
                    logging.warning('Prism backup log path does not exists or issue thread [%s] could not be found.', self.worker_thread)
                    logging.debug('Going to check root backup log path')

                    try:
                        self.is_backup_path = True
                        self.find_prism_log(logPath_object.prism_rootlog_backup_file(), self.is_backup_path)
                        logging.debug('Issue thread [%s] found in prism root backup log and will be parsed for any issue.', self.worker_thread)
                        return True
                    except subprocess.CalledProcessError as ex:
                        logging.warning('Prism root backup log path does not exists or issue thread [%s] could not be found.',self.worker_thread) 
                        logging.debug('Going to check queue_id_99 log.')
                        try:
                            self.find_prism_log(logPath_object.prism_queue_id_99_log_file(), self.is_backup_path)
                            logging.debug('Issue thread [%s] found in prism queue_id_99 log and will be parsed for any issue.', self.worker_thread)
                            return True
                        except subprocess.CalledProcessError as ex:
                            logging.warning('Prism queue_id_99 log path does not exists or issue thread [%s] could not be found.',self.worker_thread)
                            logging.error(ex)
                            logging.error('no prism logs could be found against %s or log may not be in debug mode', self.worker_thread)
                            return False
    
    def find_tomcat_log(self, logPath, is_backup_path):
        try:
            if is_backup_path:
                worker_thread_log = subprocess.check_output(f"grep {self.worker_thread} {logPath}", universal_newlines=True, shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
                record = [data for data in worker_thread_log]
            else:
                worker_thread_log = subprocess.run(["grep", f"{self.worker_thread}", f"{logPath}"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
                record = [data for data in worker_thread_log.stdout]
            with open(self.tomcat_thread_outfile, "a") as write_file:
                write_file.writelines(record)
        except subprocess.CalledProcessError as ex:
            raise
                    
    def find_prism_log(self, logPath, is_backup_path):
        try:
            if is_backup_path:
                worker_thread_log = subprocess.check_output(f"grep {self.worker_thread} {logPath}", universal_newlines=True, shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
                record = [data for data in worker_thread_log]
            else:
                worker_thread_log = subprocess.run(["grep", f"{self.worker_thread}", f"{logPath}"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
                record = [data for data in worker_thread_log.stdout]
            with open(self.prismd_thread_outfile, "a") as write_file:
                write_file.writelines(record)
        except subprocess.CalledProcessError as ex:
            raise