"""
Path finder package.
"""
import logging
from pathlib import Path
import subprocess
from subprocess import PIPE
import xml.etree.ElementTree as ET
from configparser import ConfigParser

class LogPathFinder:
    """
    Path finder class
    """
    def __init__(self, hostname, config):
        
        self.hostname = hostname
        self.config = config
        self.is_tomcat = False
        self.is_prsim = False
        self.is_sms = False

        self.is_tomcat_tlog_path = False
        self.is_prism_tlog_path = False
        self.is_sms_tlog_path = False
        self.is_access_path = False
        self.is_tomcat_daemon_path = False
        self.is_prismd_daemon_path = False
        self.is_smsd_daemon_path = False
        
        self.logger_list = []
        self.logger_dict = {}
        self.prism_log_path_dict = {}
        self.tomcat_log_path_dict = {}
        self.sms_log_path_dict = {}
        
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
        
        self.sms_base_log_path = "sms_base_log_path"
        self.sms_tlog_log_path = "sms_tlog_log_path"
        self.sms_daemon_log_path = "sms_daemon_log_path"
        self.sms_daemon_log_backup_path = "sms_daemon_log_backup_path"
        self.sms_queue_id_processor_99_log_path = "sms_queue_id_processor_99_log_path"
        self.sms_DynamicExecutorLogger_log_path = "sms_DynamicExecutorLogger_log_path"
        self.sms_DynamicExecutorLogger_log_backup_path = "sms_DynamicExecutorLogger_log_backup_path"
        self.sms_root_log_path = "sms_root_log_path"
        self.sms_root_log_backup_path = "sms_root_log_backup_path"
        self.sms_snmp_log_path = "sms_snmp_log_path"
        self.sms_snmp_log_backup_path = "sms_snmp_log_backup_path"

        # self.tomcat_conf_path = "tomcat_conf_path"
        self.tomcat_access_path = "tomcat_access_path"
        self.tomcat_base_log_path = "tomcat_base_log_path"
        self.tomcat_tlog_log_path = "tomcat_tlog_log_path"
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

    def parse_transaction_logging(self, pname):
        """
        Parse conf
        """
        # config = ConfigParser()
        config = self.config
        hostname = self.hostname
        
        try:
            if config[hostname]['PRISM']['PRISM_TOMCAT']['ACCESS_LOG_PATH'] != "":
                self.tomcat_log_path_dict[self.tomcat_access_path] = config[hostname]['PRISM']['PRISM_TOMCAT']['ACCESS_LOG_PATH']
                self.is_access_path = True
            else:
                logging.info('access log path not available in common.json file. Hence access log will not be fetched.')
        except KeyError as ex:
            logging.error('Eigther %s/PRISM/PRISM_TOMCAT/ACCESS_LOG key not present in common.json file\
                Please check with OARM team', hostname)
            logging.error('Hence access log will not be fetched.')
            
        logging.info('\n')
            
        if pname == 'tomcat':
            try:
                if config[hostname]['PRISM']['PRISM_TOMCAT']['TRANS_BASE_DIR'] != "":
                    self.tomcat_log_path_dict[self.tomcat_base_log_path] = f"{config[hostname]['PRISM']['PRISM_TOMCAT']['TRANS_BASE_DIR']}/"
                    self.tomcat_log_path_dict[self.tomcat_tlog_log_path] = f"{config[hostname]['PRISM']['PRISM_TOMCAT']['TRANS_BASE_DIR']}/TLOG/"
                    self.is_tomcat_tlog_path = True
                else:
                    logging.error('tomcat TRANS_BASE_DIR path not available common.json file, hence tomcat tlog will not be fetched.') 
            
                if config[hostname]['PRISM']['PRISM_TOMCAT']['LOG4J2_XML'] != "":
                    self.parse_logger(config[hostname]['PRISM']['PRISM_TOMCAT']['LOG4J2_XML'], pname)
                    self.is_tomcat_daemon_path = True
                else:
                    logging.error('tomcat LOG4J2_XML not configured in common.json file')
            except KeyError as ex:
                logging.error('TRANS_BASE_DIR key not present in common.json file')

        elif pname == 'prismd':
            try:
                if config[hostname]['PRISM']['PRISM_DEAMON']['TRANS_BASE_DIR'] != "":
                    self.prism_log_path_dict[self.prism_base_log_path] = f"{config[hostname]['PRISM']['PRISM_DEAMON']['TRANS_BASE_DIR']}/"
                    self.prism_log_path_dict[self.prism_tlog_log_path] = f"{config[hostname]['PRISM']['PRISM_DEAMON']['TRANS_BASE_DIR']}/TLOG/"
                    self.is_prism_tlog_path = True
                else:
                    logging.error('prismd TRANS_BASE_DIR path not present common.json file, hence prismd tlog will not be fetched.') 
                
                if config[hostname]['PRISM']['PRISM_DEAMON']['LOG4J2_XML'] != "":
                    self.parse_logger(config[hostname]['PRISM']['PRISM_DEAMON']['LOG4J2_XML'], pname)
                    self.is_prismd_daemon_path = True
                else:
                    logging.error('prismd LOG4J2_XML not configured in common.json file')
            except KeyError as ex:
                logging.error('TRANS_BASE_DIR key not present in common.json file')
                            
        elif pname == 'smsd':
            try:
                if config[hostname]['PRISM']['PRISM_SMSD']['TRANS_BASE_DIR'] != "":
                    self.sms_log_path_dict[self.sms_base_log_path] = f"{config[hostname]['PRISM']['PRISM_SMSD']['TRANS_BASE_DIR']}/"
                    self.sms_log_path_dict[self.sms_tlog_log_path] = f"{config[hostname]['PRISM']['PRISM_SMSD']['TRANS_BASE_DIR']}/TLOG/"
                    self.is_sms_tlog_path = True
                else:
                    logging.error('smsd TRANS_BASE_DIR path not present common.json file, hence smsd tlog will not be fetched.') 
                
                if config[hostname]['PRISM']['PRISM_SMSD']['LOG4J2_XML'] != "":
                    self.parse_logger(config[hostname]['PRISM']['PRISM_SMSD']['LOG4J2_XML'], pname)
                    self.is_smsd_daemon_path = True
                else:
                    logging.error('smsd LOG4J2_XML not configured in config.properties')
            except KeyError as ex:
                logging.error('TRANS_BASE_DIR key not present in common.json file')
                    

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

                            elif pname == "prismd":
                                self.prism_log_path_dict[self.prism_daemon_log_path] = appender.attrib.get('fileName')
                                append = appender.attrib.get('filePattern').split("$$")
                                self.prism_log_path_dict[self.prism_daemon_log_backup_path] = append[0]
                                
                            elif pname == "smsd":
                                self.sms_log_path_dict[self.sms_daemon_log_path] = appender.attrib.get('fileName')
                                append = appender.attrib.get('filePattern').split("$$")
                                self.sms_log_path_dict[self.sms_daemon_log_backup_path] = append[0]
                                
                        
                        elif data.attrib.get('name') == 'DynamicExecutorLogger':
                            if pname == "tomcat":
                                self.tomcat_log_path_dict[self.tomcat_DynamicExecutorLogger_log_path] = appender.attrib.get('fileName')
                                append = appender.attrib.get('filePattern').split("$$")
                                self.tomcat_log_path_dict[self.tomcat_DynamicExecutorLogger_log_backup_path] = append[0]

                            elif pname == "prismd":
                                self.prism_log_path_dict[self.prism_DynamicExecutorLogger_log_path] = appender.attrib.get('fileName')
                                append = appender.attrib.get('filePattern').split("$$")
                                self.prism_log_path_dict[self.prism_DynamicExecutorLogger_log_backup_path] = append[0]
                            
                            elif pname == "smsd":
                                self.sms_log_path_dict[self.sms_DynamicExecutorLogger_log_path] = appender.attrib.get('fileName')
                                append = appender.attrib.get('filePattern').split("$$")
                                self.sms_log_path_dict[self.sms_DynamicExecutorLogger_log_backup_path] = append[0]
                    
                        elif str(logger.attrib.get('ref')).lower() == "root":
                            if pname == "tomcat":
                                self.tomcat_log_path_dict[self.tomcat_root_log_path] = appender.attrib.get('fileName')
                                append = appender.attrib.get('filePattern').split("$$")
                                self.tomcat_log_path_dict[self.tomcat_root_log_backup_path] = append[0]

                            elif pname == "prismd":
                                    self.prism_log_path_dict[self.prism_root_log_path] = appender.attrib.get('fileName')
                                    append = appender.attrib.get('filePattern').split("$$")
                                    self.prism_log_path_dict[self.prism_root_log_backup_path] = append[0]
                            
                            elif pname == "smsd":
                                    self.sms_log_path_dict[self.sms_root_log_path] = appender.attrib.get('fileName')
                                    append = appender.attrib.get('filePattern').split("$$")
                                    self.sms_log_path_dict[self.sms_root_log_backup_path] = append[0]

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
                            
                            elif pname == "prismd":
                                self.prism_log_path_dict[self.prism_snmp_log_path] = appender.attrib.get('fileName')
                                append = appender.attrib.get('filePattern').split("$$")
                                self.prism_log_path_dict[self.prism_snmp_log_backup_path] = append[0]    
                            
                            elif pname == "smsd":
                                self.sms_log_path_dict[self.sms_snmp_log_path] = appender.attrib.get('fileName')
                                append = appender.attrib.get('filePattern').split("$$")
                                self.sms_log_path_dict[self.sms_snmp_log_backup_path] = append[0]    

                    else:
                        for appender_routing in tree.findall('./Appenders/Routing'):
                            if logger.attrib.get('ref') == appender_routing.attrib.get('name'):
                                for appender_routing in tree.findall('./Appenders/Routing/Routes/Route/File'):
                                    q_id = appender_routing.attrib.get('fileName')
                                    queue_id = q_id.split("$")
                                    if pname == "tomcat":
                                        self.tomcat_log_path_dict[self.tomcat_queue_id_processor_99_log_path] = f"{queue_id[0]}PROCESSOR_99.log"
                                    elif pname == "prismd":
                                        self.prism_log_path_dict[self.prism_queue_id_processor_99_log_path] =  f"{queue_id[0]}PROCESSOR_99.log"
                                    elif pname == "smsd":
                                        self.sms_log_path_dict[self.sms_queue_id_processor_99_log_path] =  f"{queue_id[0]}PROCESSOR_99.log"
        except ET.ParseError as ex:
            logging.debug(ex)
    
    def initialize_tomcat_path(self, tomcat):
        """
        Initialize tomcat path.
        """
        try:
            self.parse_transaction_logging(tomcat)
        except ValueError as error:
            raise ValueError(error)
        except Exception as error:
            raise

    def initialize_prism_path(self, prismd):
        """
        Initialize prism path
        """
        try:
            self.parse_transaction_logging(prismd)
        except ValueError as error:
            raise ValueError(error)
        except Exception as error:
            raise
    
    def initialize_sms_path(self, smsd):
        """
        Initialize sms path
        """
        try:
            self.parse_transaction_logging(smsd)
        except ValueError as error:
            raise ValueError(error)
        except Exception as error:
            raise