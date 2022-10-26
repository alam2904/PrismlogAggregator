"""
Path finder package.
"""
import logging
import subprocess
from subprocess import PIPE
from sys import stderr
from typing_extensions import Self
import xml.etree.ElementTree as ET

class LogPathFinder():
    """
    Path finder class
    """
    def __init__(self):

        self.is_tomcat = False
        self.is_prsim = False
        self.is_sms = False

        self.is_tomcat_process_directory = False
        self.is_prsim_process_directory = False
        self.is_sms_process_directory = False

        self.is_tomcat_tlog_path = False
        self.is_prism_tlog_path = False
        self.is_sms_tlog_path = False


        
        self.dict_of_process = {}
        self.dict_of_process_dir = {"PrismD" : {"PROCESS_HOME_DIR" : "","PROCESS_CONF_DIR" : ""}, "tomcat" : {"PROCESS_HOME_DIR" : "","PROCESS_CONF_DIR" : ""}}
        
        self.logger_list = []
        self.logger_dict = {}
        self.prism_log_path_dict = {}
        
        self.prism_base_log_path = "prism_base_log_path"
        self.prism_tlog_log_path = "prism_tlog_log_path"
        self.prism_daemon_log_path = "prism_daemon_log_path"
        self.prism_daemon_log_backup_path = "prism_daemon_log_backup_path"
        self.prism_queue_id_processor_99_log_path = "prism_queue_id_processor_99_log_path"
        self.prism_DynamicExecutorLogger_log_path = "prism_DynamicExecutorLogger_log_path"
        self.prism_DynamicExecutorLogger_log_backup_path = "prism_DynamicExecutorLogger_log_backup_path"
        self.prism_root_log_path = "prism_root_log_path"
        self.prism_root_log_backup_path = "prism_root_log_backup_path"
        self.prism_snmp_log_path = "prism_snmp_log_path"
        self.prism_snmp_log_backup_path = "prism_snmp_log_backup_path"

        self.tomcat_conf_path = "tomcat_conf_path"
        self.tomcat_base_log_path = "tomcat_base_log_path"
        self.tomcat_tlog_log_path = "tomcat_tlog_log_path"
        self.tomcat_log_path_dict = {}
        self.tomcat_daemon_log_path = "tomcat_daemon_log_path"
        self.tomcat_daemon_log_backup_path = "tomcat_daemon_log_backup_path"
        self.tomcat_queue_id_processor_99_log_path = "tomcat_queue_id_processor_99_log_path"
        self.tomcat_DynamicExecutorLogger_log_path = "tomcat_DynamicExecutorLogger_log_path"
        self.tomcat_DynamicExecutorLogger_log_backup_path = "tomcat_DynamicExecutorLogger_log_backup_path"
        self.tomcat_root_log_path = "tomcat_root_log_path"
        self.tomcat_root_log_backup_path = "tomcat_root_log_backup_path"
        self.tomcat_snmp_log_path = "tomcat_snmp_log_path"
        self.tomcat_snmp_log_backup_path = "tomcat_snmp_log_backup_path"

        self.generic_server_log_path = "generic_server_log_path"
        self.generic_server_log_backup_path = "generic_server_log_backup_path"
        self.generic_server_req_resp_log_path = "generic_server_req_resp_log_path"
        self.generic_server_req_resp_log_backup_path = "generic_server_req_resp_log_backup_path"
        self.generic_server_reverse_map_log_path = "generic_server_reverse_map_log_path"
        self.generic_server_reverse_map_log_backup_path = "generic_server_reverse_map_log_backup_path"

    
    def find_tomcat_process(self):
        """
        running java processes
        """
        try:
            running_processes = subprocess.run(["jps"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
            for process in running_processes.stdout.replace("\n", "|").split("|"):
                if len(process) != 0:
                    pid, pname = tuple(process.split())
                    if pname == "Bootstrap":
                        pname = "tomcat"
                        self.is_tomcat = True
                        try:
                            self.find_process_directory(pname)
                        except Exception as error:
                            raise
                        break
            else:
                raise ValueError('tomcat process is not running')

        except Exception as error:
            raise

                
    def find_prism_process(self):

        try:
            running_processes = subprocess.run(["jps"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
            for process in running_processes.stdout.replace("\n", "|").split("|"):
                if len(process) != 0:
                    pid, pname = tuple(process.split())
                    if pname == "PrismD":
                        pname = "PrismD"
                        self.is_prsim = True
                        try:
                            self.find_process_directory(pname)
                        except Exception as error:
                            raise
                        break
            else:
                raise ValueError('prism process is not running')

        except Exception as error:
            raise
  

    def find_process_directory(self, pname):
        """
        finding processes class path
        """
        try:
            process_dir = subprocess.run(["cat", f"/etc/{pname}.cnf"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
            for content in process_dir.stdout.replace("\n", "|").split("|"):
                if len(content) != 0:
                    pname_dir, pvalue_dir = tuple(content.split("="))
                self.dict_of_process_dir[pname][pname_dir] = pvalue_dir

            if pname == "tomcat":
                self.is_tomcat_process_directory = True
                self.find_tomcat_tlog_path(pname)

            elif pname == "PrismD":
                self.is_prsim_process_directory = True
                self.find_prism_tlog_path(pname)

        except Exception as error:
            logging.error('%s.cnf not present', pname)
            raise Exception(error)

    def find_tomcat_tlog_path(self, pname):

        """
        Find and parse conf property file for tomcat log paths.
        """
        # process_name = "tomcat"
        tomcat_home_dir = self.dict_of_process_dir[pname]["PROCESS_HOME_DIR"]
        try:
            tomcat_shared_loader = subprocess.run(["grep", "shared.loader", f"{tomcat_home_dir}/conf/catalina.properties"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
            for content in tomcat_shared_loader.stdout.replace("\n", "|").split("|"):
                if len(content) != 0:
                    key, value = tuple(content.split("="))
                    tomcat_key, tomcat_value = tuple(value.split(","))
                    if tomcat_key:
                        for item in tomcat_key.split("/"):
                            if item == "conf":
                                self.tomcat_conf_path = tomcat_key
                    else:
                        self.tomcat_conf_path = tomcat_value

            self.parse_transaction_logging(self.tomcat_conf_path, pname)
        except subprocess.CalledProcessError as ex:
            logging.debug(ex)

    def find_prism_tlog_path(self, pname):
        """
        Find and parse conf property file for prism log paths.
        """
        # process_name = "PrismD"
        prism_conf = self.dict_of_process_dir[pname]["PROCESS_CONF_DIR"]
        self.parse_transaction_logging(prism_conf, pname)

    def parse_transaction_logging(self, conf, pname):
        """
        Parse conf
        """
        try:
            trans_base_dir = subprocess.run(["grep", "TRANS_BASE_DIR", f"{conf}/TransactionLogging.properties"], stdout=PIPE, stderr=PIPE, universal_newlines=True, check=True)
            for content in trans_base_dir.stdout.replace("\n", "|").split("|"):
                if len(content) != 0:
                    key, value = tuple(content.split("="))
                    if pname == "tomcat":
                        self.tomcat_log_path_dict[self.tomcat_base_log_path] = f"{value}/"
                        self.tomcat_log_path_dict[self.tomcat_tlog_log_path] = f"{value}/TLOG/"
                        self.is_tomcat_tlog_path = True

                    elif pname == "PrismD":
                        self.prism_log_path_dict[self.prism_base_log_path] = f"{value}/"
                        self.prism_log_path_dict[self.prism_tlog_log_path] = f"{value}/TLOG/"
                        self.is_prism_tlog_path = True
            
            if pname == "tomcat":
                self.find_tomcat_daemon_path(pname)

            elif pname == "PrismD":
                self.find_prism_daemon_path(pname)

        except subprocess.CalledProcessError as ex:
            logging.debug(ex)

    def find_tomcat_daemon_path(self, pname):
        """
        Find and parse log4j
        """
        # process_name = "tomcat"
        log4j = f"{self.tomcat_conf_path}/log4j2.xml"
        self.parse_logger(log4j, pname)

    def find_prism_daemon_path(self, pname):
        """
        Find and parse log4j
        """
        # process_name = "PrismD"
        prism_conf = self.dict_of_process_dir[pname]["PROCESS_CONF_DIR"]
        log4j = f"{prism_conf}/log4j2.xml"

        self.parse_logger(log4j, pname)


    def parse_logger(self, log4j, pname):
        """
        Parse appender
        """
        try:
            tree = ET.parse(log4j)
            for data in tree.findall('./Loggers/Logger'):
                if data.attrib.get('name') == 'com.onmobile.prism':
                    self.parse_appender(data, tree, pname)

                elif data.attrib.get('name') == 'DynamicExecutorLogger':
                    self.parse_appender(data, tree, pname)
                
                elif data.attrib.get('name') == 'com.onmobile.prism.generic.server'or data.attrib.get('name') == 'com.onmobile.prism.servlets.generic' or data.attrib.get('name') == 'GenericServer':
                    self.parse_appender(data, tree, pname)
                
                elif data.attrib.get('name') == 'req_res_log_act':
                    self.parse_appender(data, tree, pname)
                
                elif data.attrib.get('name') == 'reverse_map_log_act':
                    self.parse_appender(data, tree, pname)

                elif data.attrib.get('name') == 'com.onmobile.snmp':
                    self.parse_appender(data, tree, pname)

            for data in tree.findall('./Loggers/Root'):
                self.parse_appender(data, tree, pname)
        except ET.ParseError as ex:
            logging.debug(ex)

    
    def parse_appender(self, data, tree, pname):
        try:
            for logger in data.findall('AppenderRef'):
                for appender in tree.findall('./Appenders/RollingFile'):
                    if logger.attrib.get('ref') == appender.attrib.get('name'):
                        if data.attrib.get('name') == 'com.onmobile.prism':
                            if pname == "tomcat":
                                self.tomcat_log_path_dict[self.tomcat_daemon_log_path] = appender.attrib.get('fileName')
                                append = appender.attrib.get('filePattern').split("$$")
                                self.tomcat_log_path_dict[self.tomcat_daemon_log_backup_path] = append[0]

                            elif pname == "PrismD":
                                self.prism_log_path_dict[self.prism_daemon_log_path] = appender.attrib.get('fileName')
                                append = appender.attrib.get('filePattern').split("$$")
                                self.prism_log_path_dict[self.prism_daemon_log_backup_path] = append[0]
                        
                        elif data.attrib.get('name') == 'DynamicExecutorLogger':
                            if pname == "tomcat":
                                self.tomcat_log_path_dict[self.tomcat_DynamicExecutorLogger_log_path] = appender.attrib.get('fileName')
                                append = appender.attrib.get('filePattern').split("$$")
                                self.tomcat_log_path_dict[self.tomcat_DynamicExecutorLogger_log_backup_path] = append[0]

                            elif pname == "PrismD":
                                self.prism_log_path_dict[self.prism_DynamicExecutorLogger_log_path] = appender.attrib.get('fileName')
                                append = appender.attrib.get('filePattern').split("$$")
                                self.prism_log_path_dict[self.prism_DynamicExecutorLogger_log_backup_path] = append[0]
                    
                        elif str(logger.attrib.get('ref')).lower() == "root":
                            if pname == "tomcat":
                                self.tomcat_log_path_dict[self.tomcat_root_log_path] = appender.attrib.get('fileName')
                                append = appender.attrib.get('filePattern').split("$$")
                                self.tomcat_log_path_dict[self.tomcat_root_log_backup_path] = append[0]

                            elif pname == "PrismD":
                                    self.prism_log_path_dict[self.prism_root_log_path] = appender.attrib.get('fileName')
                                    append = appender.attrib.get('filePattern').split("$$")
                                    self.prism_log_path_dict[self.prism_root_log_backup_path] = append[0]

                        elif data.attrib.get('name') == 'com.onmobile.prism.generic.server'or data.attrib.get('name') == 'com.onmobile.prism.servlets.generic' or data.attrib.get('name') == 'GenericServer':
                            if pname == "tomcat":
                                self.tomcat_log_path_dict[self.generic_server_log_path] = appender.attrib.get('fileName')
                                append = appender.attrib.get('filePattern').split("$$")
                                self.tomcat_log_path_dict[self.generic_server_log_backup_path] = append[0]

                        elif data.attrib.get('name') == 'req_res_log_act':
                            if pname == "tomcat":
                                self.tomcat_log_path_dict[self.generic_server_req_resp_log_path] = appender.attrib.get('fileName')
                                append = appender.attrib.get('filePattern').split("$$")
                                self.tomcat_log_path_dict[self.generic_server_req_resp_log_backup_path] = append[0]
                        
                        elif data.attrib.get('name') == 'reverse_map_log_act':
                            if pname == "tomcat":
                                self.tomcat_log_path_dict[self.generic_server_reverse_map_log_path] = appender.attrib.get('fileName')
                                append = appender.attrib.get('filePattern').split("$$")
                                self.tomcat_log_path_dict[self.generic_server_reverse_map_log_backup_path] = append[0]

                        elif data.attrib.get('name') == 'com.onmobile.snmp':
                            if pname == "tomcat":
                                self.tomcat_log_path_dict[self.tomcat_snmp_log_path] = appender.attrib.get('fileName')
                                append = appender.attrib.get('filePattern').split("$$")
                                self.tomcat_log_path_dict[self.tomcat_snmp_log_backup_path] = append[0]
                            
                            elif pname == "PrismD":
                                self.prism_log_path_dict[self.prism_snmp_log_path] = appender.attrib.get('fileName')
                                append = appender.attrib.get('filePattern').split("$$")
                                self.prism_log_path_dict[self.prism_snmp_log_backup_path] = append[0]    

                    else:
                        for appender_routing in tree.findall('./Appenders/Routing'):
                            if logger.attrib.get('ref') == appender_routing.attrib.get('name'):
                                for appender_routing in tree.findall('./Appenders/Routing/Routes/Route/File'):
                                    q_id = appender_routing.attrib.get('fileName')
                                    queue_id = q_id.split("$")
                                    if pname == "tomcat":
                                        self.tomcat_log_path_dict[self.tomcat_queue_id_processor_99_log_path] = f"{queue_id[0]}PROCESSOR_99.log"
                                    elif pname == "PrismD":
                                        self.prism_log_path_dict[self.prism_queue_id_processor_99_log_path] =  f"{queue_id[0]}PROCESSOR_99.log"
        except ET.ParseError as ex:
            logging.debug(ex)


    def initialize_tomcat_path(self):
        """
        Initialize tomcat path.
        """
        try:
            self.find_tomcat_process()
        except ValueError as error:
            raise ValueError(error)
        except Exception as error:
            raise

    def initialize_prism_path(self):
        """
        Initialize prism path
        """
        try:
            self.find_prism_process()
        except ValueError as error:
            raise ValueError(error)
        except Exception as error:
            raise


        # self.find_tomcat_tlog_path()
        # self.find_tomcat_daemon_path()
        # self.find_prism_tlog_path()
        # self.find_prism_daemon_path()