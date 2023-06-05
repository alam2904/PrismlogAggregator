import logging
import os
import socket
from input_tags import PrismHandlerClass
import xml.etree.ElementTree as ET
from outfile_writer import FileWriter

class HandlerFileProcessor:
    def __init__(self, config, handler_info, outputDirectory_object, oarm_uid):
        self.config = config
        self.handler_info = handler_info
        self.outputDirectory_object = outputDirectory_object
        self.oarm_uid = oarm_uid
        
        self.hostname = socket.gethostname()
        self.params = []
        self.handler_files = []
        self.web_services = []
        self.prism_deamon_conf_path = ""
        self.prism_tmcat_conf_path = ""
    
    def getHandler_files(self):
        try:
            for handlers in self.handler_info["HANDLER_INFO"]:
                for handler in handlers:
                    for h_name, h_class in PrismHandlerClass.__dict__.items():
                        if not h_name.startswith("__"):
                            if handler["handler_name"] == h_class: 
                                # logging.info('handler params: %s', handler["params"])
                                self.params.append(handler["params"])
        except Exception as ex:
            logging.info(ex)
            
        if self.params:
            self.parse_params()
        
    def parse_params(self):
        try:
            if self.config[self.hostname]["PRISM"]["PRISM_DEAMON"]["PRISM_DEAMON"]["CONF_PATH"]:
                self.prism_deamon_conf_path = self.config[self.hostname]["PRISM"]["PRISM_DEAMON"]["PRISM_DEAMON"]["CONF_PATH"]    
        except KeyError as error:
            logging.error(error)
        
        try:
            for webService in self.config[self.hostname]["PRISM"]["PRISM_TOMCAT"]:
                self.web_services.append(webService)
                logging.info('tomcat web services: %s', self.web_services)
        
                try:
                    if self.config[self.hostname]['PRISM']['PRISM_TOMCAT'][webService]['CONF_PATH'] != "":
                        self.prism_tmcat_conf_path = self.config[self.hostname]['PRISM']['PRISM_TOMCAT'][webService]['CONF_PATH']
                    else:
                        logging.info('%s conf path not available in %s.json file', webService, self.hostname)
                except KeyError as error:
                    logging.exception(error)
                    logging.error('Hence %s conf path will not be fetched.', webService)
        except KeyError as error:
            logging.exception(error)
            
        try:
            for param in self.params:
                root = ET.fromstring(param)
                # Retrieve the value of the XML_PATH attribute
                self.handler_files.append(root.find('params').get('XML_PATH'))
                
                velocity_path = self.find_absolute_velocity_prop_path(root, 'VELOCITY_PROP_FILE_PATH')
                logging.info("VELOCITY_PROP_FILE_PATH: %s", velocity_path)
                if velocity_path:
                    self.handler_files.append(velocity_path)
                    
                self.handler_files.append(root.find('params').get('RESPONSE_FILE'))
                
                self.handler_files.append(root.find('params').get('REQUEST_FILE'))
                self.handler_files.append(root.find('params').get('CONF_FILE'))
                self.handler_files.append(root.find('params').get('RESPONSE_FILE'))
                self.handler_files.append(root.find('params').get('DYN_EXEC_FILE'))
                
                self.handler_files.append(root.find('params').get('TEMPLATE_XML_PATH'))
                
                velocity_path = self.find_absolute_velocity_prop_path(root, 'VELOCITY_PROP_FILE')
                logging.info("VELOCITY_PROP_FILE: %s", velocity_path)
                
                if velocity_path:
                    self.handler_files.append(velocity_path)
                
                logging.info("HANDLER_FILES: %s", self.handler_files)
        except ET.ParseError as ex:
            logging.debug(ex)
        
        if self.handler_files:
            self.copy_handler_files()
    
    def copy_handler_files(self):
        fileWriter_object = FileWriter(self.outputDirectory_object, self.oarm_uid)
        
        folder = os.path.join(self.outputDirectory_object, "{}_issue_handler_files".format(self.hostname))
        self.create_folder(folder)
        
        fileWriter_object.write_handler_files(self.handler_files, folder)
    
    def find_absolute_velocity_prop_path(self, root, properties):
        path = ""
        
        if self.prism_deamon_conf_path:
            path = os.path.join(self.prism_deamon_conf_path, '{}'.format(root.find('params').get('{}'.format(properties))))
            
            if os.path.exists(path):
                return path
    
        if self.prism_tmcat_conf_path:
            path = os.path.join(self.prism_tmcat_conf_path, '{}'.format(root.find('params').get('{}'.format(properties))))
            
            if os.path.exists(path):
                return path
        return path
            
    def create_folder(self, folder):
        """
            creating process folder
        """
        try:
            if not os.path.exists(folder):
                os.mkdir(folder)
        except os.error as error:
            logging.info(error)
            os.mkdir(folder)
            # folder.mkdir(parents=True)
        
                            