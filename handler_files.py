from collections import defaultdict
import logging
import os
import re
import socket
from status_tags import PrismHandlerClass
import xml.etree.ElementTree as ET
from outfile_writer import FileWriter

class HandlerFileProcessor:
    def __init__(self, config, handler_info, outputDirectory_object, oarm_uid):
        self.config = config
        self.handler_info = handler_info
        self.outputDirectory_object = outputDirectory_object
        self.oarm_uid = oarm_uid
        
        self.hostname = socket.gethostname()
        self.params = defaultdict(list)
        self.handler_files = []
        self.web_services = []
        self.prism_deamon_conf_path = ""
        self.prism_tomcat_conf_path = ""
        self.macro_name = []
        self.cdr_file_id = ""
    
    def getHandler_files(self):
        try:
            for handlers in self.handler_info["HANDLER_INFO"]:
                for handler in handlers:
                    for h_name, h_class in PrismHandlerClass.__dict__.items():
                        if not h_name.startswith("__"):
                            if handler["handler_name"] == h_class: 
                                # logging.info('handler params: %s', handler["params"])
                                # self.params.append(handler["params"])
                                self.params[h_name].append(handler["params"])
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
                        self.prism_tomcat_conf_path = self.config[self.hostname]['PRISM']['PRISM_TOMCAT'][webService]['CONF_PATH']
                    else:
                        logging.info('%s conf path not available in %s.json file', webService, self.hostname)
                except KeyError as error:
                    logging.exception(error)
                    logging.error('Hence %s conf path will not be fetched.', webService)
        except KeyError as error:
            logging.exception(error)
            
        try:
            for h_name, params in self.params.items():
                for param in params:
                    root = ET.fromstring(param)
                    # Retrieve the value of the path attributes
                    
                    if h_name == "GENERIC_HTTP" or h_name == "GENERIC_AUTH":
                        if root.find('params').get('XML_PATH') != None:
                            self.handler_files.append(root.find('params').get('XML_PATH'))
                        
                        self.find_absolute_velocity_prop_path(root, 'VELOCITY_PROP_FILE_PATH')
                            
                        if ( root.find('params').get('RESPONSE_FILE') != None 
                            and root.find('params').get('RESPONSE_FILE') not in self.handler_files):
                            self.handler_files.append(root.find('params').get('RESPONSE_FILE'))
                    
                    elif h_name == "GENERIC_SOAP":
                        if root.find('params').get('REQUEST_FILE') != None:
                            self.handler_files.append(root.find('params').get('REQUEST_FILE'))
                        
                        if root.find('params').get('CONF_FILE') != None:
                            self.handler_files.append(root.find('params').get('CONF_FILE'))
                        
                        if ( root.find('params').get('RESPONSE_FILE') != None 
                            and root.find('params').get('RESPONSE_FILE') not in self.handler_files):
                            self.handler_files.append(root.find('params').get('RESPONSE_FILE'))
                        
                        if root.find('params').get('DYN_EXEC_FILE') != None:
                            self.handler_files.append(root.find('params').get('DYN_EXEC_FILE'))
                    
                    elif h_name == "GENERIC_CDR":
                        if root.find('params').get('TEMPLATE_XML_PATH') != None:
                            self.handler_files.append(root.find('params').get('TEMPLATE_XML_PATH'))
                        
                        if root.find('params').get('FILE_ID') != None:
                            self.cdr_file_id = root.find('params').get('FILE_ID')
                        
                        self.find_absolute_velocity_prop_path(root, 'VELOCITY_PROP_FILE')
                    
            logging.info("HANDLER_FILES: %s", self.handler_files)
        except ET.ParseError as ex:
            logging.debug(ex)
        
        if self.handler_files:
            self.copy_handler_files()
    
    def copy_handler_files(self):
        fileWriter_object = FileWriter(self.outputDirectory_object, self.oarm_uid)
        
        folder = os.path.join(self.outputDirectory_object, "{}_handler_files".format(self.hostname))
        self.create_folder(folder)
        
        fileWriter_object.write_files(self.handler_files, folder)
    
    def find_absolute_velocity_prop_path(self, root, properties):
        path = ""
        
        if self.prism_deamon_conf_path and root.find('params').get('{}'.format(properties)) != None:
            path = os.path.join(self.prism_deamon_conf_path, '{}'.format(root.find('params').get('{}'.format(properties))))
            
            if os.path.exists(path):
                logging.info("PRISM_VELOCITY_PROP_FILE_PATH: %s", path)
                self.handler_files.append(path)
                self.get_macro_file(path)
    
        if self.prism_tomcat_conf_path and root.find('params').get('{}'.format(properties)) != None:
            path = os.path.join(self.prism_tomcat_conf_path, '{}'.format(root.find('params').get('{}'.format(properties))))
            
            if os.path.exists(path):
                logging.info("TOMCAT_VELOCITY_PROP_FILE_PATH: %s", path)
                self.handler_files.append(path)
                self.get_macro_file(path)
    
    def get_macro_file(self, velocity_path):
        if velocity_path:
            file_resource_loader_path = []
            velocimacro_library = []
            macro_path = ""
            with open(str(velocity_path), 'r') as properties:
                file_resource_loader_path = [loader_path.split("=")[1] for loader_path in properties.readlines() if re.search("file.resource.loader.path", loader_path, re.DOTALL)]
                velocimacro_library = [macro_name.split("=")[1].strip() for macro_name in properties.readlines() if re.search("^velocimacro.library", macro_name, re.DOTALL)]
                            
            with open(str(velocity_path), 'r') as properties:
                velocimacro_library = [macro_name.split("=")[1].strip() for macro_name in properties.readlines() if re.search("^velocimacro.library", macro_name, re.DOTALL)]
                logging.info("macro_name1st: %s", velocimacro_library)
            
            if file_resource_loader_path and velocimacro_library:
                for macro_file_path in file_resource_loader_path:
                    if macro_file_path.startswith("/"):
                        for macro_name in velocimacro_library:
                            self.macro_name.append(macro_name)
                            logging.info("file_resource_loader_path: %s", macro_file_path)
                            logging.info("macro_name: %s", macro_name)
                            macro_path = macro_file_path.replace("\n", "") + "/" + macro_name.replace("\n", "")
                    
            if macro_path:
                self.handler_files.append(macro_path)
            
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
        
                            