from datetime import datetime
import logging
import xml.etree.ElementTree as ET

class LogPathFinder():
    """
    Path finder class
    """
    def __init__(self, hostname, config, validation_object):
        
        self.config = config
        self.validation_object = validation_object
        self.start_date = validation_object.start_date
        self.end_date = validation_object.end_date
        self.hostname = hostname
        self.debugMsisdn = ""
        self.web_services = []
        
        #prism dictionary objects
        self.prism_tomcat_log_path_dict = {}
        self.prism_daemon_log_path_dict = {}
        self.prism_smsd_log_path_dict = {}
        
        #prism catalina home and access path paramter, tlog , req-resp path parameters
        self.prism_tomcat_process_home_directory = "prism_tomcat_process_home_directory"
        self.prism_deamon_process_home_directory = "prism_deamon_process_home_directory"
        self.prism_smsd_process_home_directory = "prism_smsd_process_home_directory"
        self.prism_tomcat_access_path = "prism_tomcat_access_path"
        self.prism_tomcat_tlog_path = "prism_tomcat_tlog_path"
        self.prism_daemon_tlog_path = "prism_daemon_tlog_path"
        self.prism_smsd_tlog_path = "prism_smsd_tlog_path"
        self.prism_tomcat_generic_http_handler_req_resp_path = "prism_tomcat_generic_http_handler_req_resp_path"
        self.prism_tomcat_generic_soap_handler_req_resp_path = "prism_tomcat_generic_soap_handler_req_resp_path"
        self.prism_tomcat_callbackV2_req_resp_path = "prism_tomcat_callbackV2_req_resp_path"
        self.prism_tomcat_req_resp_path = "prism_tomcat_req_resp_path"
        self.prism_tomcat_perf_log_path = "prism_tomcat_perf_log_path"
        self.prism_daemon_generic_http_handler_req_resp_path = "prism_daemon_generic_http_handler_req_resp_path"
        self.prism_daemon_generic_soap_handler_req_resp_path = "prism_daemon_generic_soap_handler_req_resp_path"
        self.prism_daemon_callbackV2_req_resp_path = "prism_daemon_callbackV2_req_resp_path"
        self.prism_daemon_req_resp_path = "prism_daemon_req_resp_path"
        self.prism_daemon_perf_log_path = "prism_daemon_perf_log_path"
        self.generic_server_request_bean_response = "generic_server_request_bean_response"
        
        #boolean path paramters
        self.is_routing_success = False
        self.is_prism_access_path = False
        self.is_debug_msisdn = False
        
        
    def parse_transaction_logging(self, process_name):
        """
        Parse log paths
        """
        search_date = datetime.strftime(self.start_date, "yyyy-MM-dd")
        pname = process_name
        log4j2_path = ""
        
        if pname == "PRISM_TOMCAT":
            try:
                for webService in self.config[self.hostname]["PRISM"]["PRISM_TOMCAT"]:
                    self.web_services.append(webService)
                    logging.info('tomcat web services: %s', self.web_services)
            
                    try:
                        if self.config[self.hostname]['PRISM']['PRISM_TOMCAT'][webService]['LOGS_PATH']['ACCESS_LOG_PATH'] != "":
                            access_log_path = self.config[self.hostname]['PRISM']['PRISM_TOMCAT'][webService]['LOGS_PATH']['ACCESS_LOG_PATH']
                            self.prism_tomcat_log_path_dict[self.prism_tomcat_access_path] = access_log_path
                            self.is_prism_access_path = True
                        else:
                            logging.info('%s access log path not available in %s.json file.'\
                                            'Hence access log will not be fetched.', webService, self.hostname)
                    except KeyError as error:
                        logging.exception(error)
                        logging.error('Hence %s access log will not be fetched.', webService)
                
                    if self.config[self.hostname]['PRISM']['PRISM_TOMCAT'][webService]['LOGS_PATH']['TRANS_BASE_DIR'] != "":
                        transBaseDirectory = self.config[self.hostname]['PRISM']['PRISM_TOMCAT'][webService]['LOGS_PATH']['TRANS_BASE_DIR']
                        
                        self.prism_tomcat_log_path_dict[self.prism_tomcat_tlog_path] = "{}/TLOG/BILLING_REALTIME".format(transBaseDirectory)
                        self.prism_tomcat_log_path_dict[self.prism_tomcat_generic_http_handler_req_resp_path] = "{}/TLOG/REQUEST_RESPONSE_GENERIC_HTTP".format(transBaseDirectory)
                        self.prism_tomcat_log_path_dict[self.prism_tomcat_generic_soap_handler_req_resp_path] = "{}/TLOG/REQUEST_RESPONSE".format(transBaseDirectory)
                        self.prism_tomcat_log_path_dict[self.prism_tomcat_callbackV2_req_resp_path] = "{}/TLOG/CBCK-V2-REQ-RESPONSE".format(transBaseDirectory)
                        self.prism_tomcat_log_path_dict[self.prism_tomcat_req_resp_path] = "{}/TLOG/REQUEST_LOG".format(transBaseDirectory)
                        self.prism_tomcat_log_path_dict[self.prism_tomcat_perf_log_path] = "{}/TLOG/PERF".format(transBaseDirectory)
                        self.prism_tomcat_log_path_dict[self.generic_server_request_bean_response] = "{}/TLOG/GENERIC_SERVER_REQUEST_BEAN_RESPONSE".format(transBaseDirectory)

                        # self.is_tomcat_tlog_path = True
                    else:
                        logging.error('%s TRANS_BASE_DIR path not available in %s.json file, hence tomcat tlog path will not be processed', webService, self.hostname) 
                    
                    if self.config[self.hostname]['PRISM']['PRISM_TOMCAT'][webService]['LOGS_PATH']['LOG4J2_XML'] != "":
                        log4j2_path = self.config[self.hostname]['PRISM']['PRISM_TOMCAT'][webService]['LOGS_PATH']['LOG4J2_XML']
                        self.parse_logger(log4j2_path, pname)
                    else:
                        logging.error('%s LOG4J2_XML path not present in %s.json file', webService, self.hostname)
                        logging.error('Hence %s LOG4J2_XML log will not be fetched for parsing and initializing logs path.', webService)
            except KeyError as error:
                logging.exception(error)
                # logging.error('Hence LOG4J2_XML log will not be fetched for parsing and initializing logs path.')
        
        if pname == "PRISM_DEAMON":
            try:    
                if self.config[self.hostname]['PRISM']['PRISM_DEAMON']['PRISM_DEAMON']['LOGS_PATH']['TRANS_BASE_DIR'] != "":
                    transBaseDirectory = self.config[self.hostname]['PRISM']['PRISM_DEAMON']['PRISM_DEAMON']['LOGS_PATH']['TRANS_BASE_DIR']
                    
                    self.prism_daemon_log_path_dict[self.prism_daemon_tlog_path] = "{}/TLOG/BILLING".format(transBaseDirectory)
                    self.prism_daemon_log_path_dict[self.prism_daemon_generic_http_handler_req_resp_path] = "{}/TLOG/REQUEST_RESPONSE_GENERIC_HTTP".format(transBaseDirectory)
                    self.prism_daemon_log_path_dict[self.prism_daemon_generic_soap_handler_req_resp_path] = "{}/TLOG/REQUEST_RESPONSE".format(transBaseDirectory)
                    self.prism_daemon_log_path_dict[self.prism_daemon_callbackV2_req_resp_path] = "{}/TLOG/CBCK-V2-REQ-RESPONSE".format(transBaseDirectory)
                    self.prism_daemon_log_path_dict[self.prism_daemon_req_resp_path] = "{}/TLOG/REQUEST_LOG".format(transBaseDirectory)
                    self.prism_daemon_log_path_dict[self.prism_daemon_perf_log_path] = "{}/TLOG/PERF".format(transBaseDirectory)

                    # self.is_tomcat_tlog_path = True
                else:
                    logging.error('%s TRANS_BASE_DIR path not available in %s.json file, hence prismd tlog will not be fetched', pname, self.hostname) 
                
                if self.config[self.hostname]['PRISM']['PRISM_DEAMON']['PRISM_DEAMON']['LOGS_PATH']['LOG4J2_XML'] != "":
                    log4j2_path = self.config[self.hostname]['PRISM']['PRISM_DEAMON']['PRISM_DEAMON']['LOGS_PATH']['LOG4J2_XML']
                    self.parse_logger(log4j2_path, pname)
                else:
                    logging.error('%s LOG4J2_XML path not present in %s.json file', pname, self.hostname)
                    logging.error('Hence %s LOG4J2_XML log will not be fetched for parsing and initializing logs path.', pname)
            except KeyError as error:
                logging.exception(error)
                # logging.error('Hence LOG4J2_XML log will not be fetched for parsing and initializing logs path.')
        
        if pname == "PRISM_SMSD":
            try:
                if self.config[self.hostname]['PRISM']['PRISM_SMSD']['PRISM_SMSD']['LOGS_PATH']['TRANS_BASE_DIR'] != "":
                    transBaseDirectory = self.config[self.hostname]['PRISM']['PRISM_SMSD']['PRISM_SMSD']['LOGS_PATH']['TRANS_BASE_DIR']
                    self.prism_smsd_log_path_dict[self.prism_smsd_tlog_path] = "{}/TLOG/SMS".format(transBaseDirectory)

                    # self.is_tomcat_tlog_path = True
                else:
                    logging.error('%s TRANS_BASE_DIR path not available in %s.json file, hence smsd tlog will not be fetched', pname, self.hostname) 
                
                if self.config[self.hostname]['PRISM']['PRISM_SMSD']['PRISM_SMSD']['LOGS_PATH']['LOG4J2_XML'] != "":
                    log4j2_path = self.config[self.hostname]['PRISM']['PRISM_SMSD']['PRISM_SMSD']['LOGS_PATH']['LOG4J2_XML']
                    self.parse_logger(log4j2_path, pname)
                else:
                    logging.error('%s LOG4J2_XML path not present in %s.json file', pname, self.hostname)
                    logging.error('Hence %s LOG4J2_XML log will not be fetched for parsing and initializing logs path.', pname)
                    
            except KeyError as error:
                logging.exception(error)
                # logging.error('Hence LOG4J2_XML log will not be fetched for parsing and initializing logs path.')
            
    
    def parse_logger(self, log4j2_path, pname):
        """
        Logger reference call to appender
        """
        try:
            tree = ET.parse(log4j2_path)
            for data in tree.findall('./Loggers/Logger'):
                self.parse_appender(data, tree, pname)
            
            for data in tree.findall('./Loggers/Root'):
                self.parse_appender(data, tree, pname)
        except ET.ParseError as ex:
            logging.debug(ex)
                  
    def parse_appender(self, data, tree, pname):
        """
        Parse appender for loggers reference
        """
        logger_ref = ""
        try:            
            for logger in data.findall('AppenderRef'):                        
                logger_ref = str(logger.attrib.get('ref'))
                
                for routing in tree.findall('./Appenders/Routing'):
                    if logger_ref == str(routing.attrib.get('name')):
                        for routes in tree.findall('./Appenders/Routing/Routes'):
                            #call to routing for re-routing the references
                            self.parse_routing_appender(routes, pname, logger_ref, routing)
                            
                        if self.is_routing_success:
                            break  
                else:
                    for appender in tree.findall('./Appenders/RollingFile'):
                        if logger_ref == str(appender.attrib.get('name')):
                            yearAndmonth = datetime.strftime(self.start_date, 'yyyy-MM')
                            search_date = datetime.strftime(self.start_date, "yyyy-MM-dd")
                            
                            replacedValue = str(appender.attrib.get('filePattern'))\
                                                .replace("$${date:yyyy-MM}", '{}'.format(yearAndmonth))\
                                                .replace("%d{yyyy-MM-dd.HH}-%i", '{}*'.format(search_date))
                                    
                            if pname == 'PRISM_TOMCAT':
                                self.prism_tomcat_log_path_dict["prism_tomcat_{0}_log".format(logger_ref)] = appender.attrib.get('fileName')
                                self.prism_tomcat_log_path_dict["prism_tomcat_{0}_backup_log".format(logger_ref)] = replacedValue
                            
                            elif pname == 'PRISM_DEAMON':
                                self.prism_daemon_log_path_dict["prism_daemon_{0}_log".format(logger_ref)] = appender.attrib.get('fileName')
                                self.prism_daemon_log_path_dict["prism_daemon_{0}_backup_log".format(logger_ref)] = replacedValue
                            
                            elif pname == 'PRISM_SMSD':
                                self.prism_smsd_log_path_dict["prism_smsd_{0}_log".format(logger_ref)] = appender.attrib.get('fileName')
                                self.prism_smsd_log_path_dict["prism_smsd_{0}_backup_log".format(logger_ref)] = replacedValue
                                    
                    # else:
                    #     logging.info('No Appender defined for the logger: %s', logger_ref)
            
        except ET.ParseError as ex:
            logging.debug(ex)
        
    def parse_routing_appender(self, data, pname, logger_ref, routing):
        """
        Re-route for logger reference and parse appender
        """
        try:
            for route in data.findall('Route'):
                self.reinitialize_is_debug_msisdn()
                
                replacedValue = ""
                if route.attrib.get('key') == 'TEST_{}'.format(self.validation_object.fmsisdn) and logger_ref == str(routing.attrib.get('name')):
                    for file in route.findall('File'):
                        replacedValue = str(file.attrib.get('fileName'))\
                                            .replace("${ctx:SUB_ID}", "{}".format(route.attrib.get('key')))
                                                    
                    if pname == "PRISM_TOMCAT":
                        self.prism_tomcat_log_path_dict["prism_tomcat_{0}_log".format(route.attrib.get('key'))] = replacedValue
                        break
                    
                    elif pname == "PRISM_DEAMON":
                        self.prism_daemon_log_path_dict["prism_daemon_{0}_log".format(route.attrib.get('key'))] = replacedValue
                        break
                    self.is_debug_msisdn = True
                
                elif route.attrib.get('key') == 'PROCESSOR_99' and logger_ref == str(routing.attrib.get('name')):
                    for file in route.findall('File'):
                        replacedValue = str(file.attrib.get('fileName'))\
                                            .replace("${ctx:QUEUE_ID}", "{}".format(route.attrib.get('key')))
                                                    
                    if pname == "PRISM_TOMCAT":
                        self.prism_tomcat_log_path_dict["prism_tomcat_{0}_log".format(route.attrib.get('key'))] = replacedValue
                        break
                    elif pname == "PRISM_DEAMON":
                        self.prism_daemon_log_path_dict["prism_daemon_{}_log".format(route.attrib.get('key'))] = replacedValue
                        break
                    elif pname == "PRISM_SMSD":
                        self.prism_smsd_log_path_dict["prism_smsd_{}_log".format(route.attrib.get('key'))] = replacedValue
                        break
                self.is_routing_success = True
                    
        except ET.ParseError as ex:
            logging.debug(ex)

    
    def reinitialize_is_debug_msisdn(self):
        self.is_debug_msisdn = False
        
    def initialize_path(self, pname):
            """
            Initialize tomcat path.
            """
            try:
                self.parse_transaction_logging(pname)
            except ValueError as error:
                raise ValueError(error)
            except Exception as error:
                raise