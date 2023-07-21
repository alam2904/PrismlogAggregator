from collections import defaultdict
import logging
import socket
from tlog_db_processor import TlogProcessor
from outfile_writer import FileWriter


class PROCESSOR:
    def __init__(self, initializedPath_object, outputDirectory_object,\
                    validation_object, oarm_uid, config):
        
        self.initializedPath_object = initializedPath_object
        self.outputDirectory_object = outputDirectory_object
        self.validation_object = validation_object
        self.oarm_uid = oarm_uid
        self.config = config
        
        #for dumping data as json
        self.prism_data_dict_list = defaultdict(list)
        self.hostname = socket.gethostname()
    
    def process(self):
        tlogProcessor_object = TlogProcessor(self.initializedPath_object, self.outputDirectory_object,\
                                        self.validation_object, self.config,\
                                        self.prism_data_dict_list)
        
        
        for pname in self.config[self.hostname]:  
            if pname == 'PRISM':
                try:
                    if self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_tlog_path"]:
                        logging.debug('%s tomcat tlog path exists', pname)
                        if tlogProcessor_object.process_tlog_db_enteries("PRISM_TOMCAT"):
                            pass
                except KeyError as error:
                    logging.exception(error)
                
                try:
                    if self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_tlog_path"]:
                        logging.debug('%s daemon tlog path exists', pname)
                        if tlogProcessor_object.process_tlog_db_enteries("PRISM_DEAMON"):
                            pass
                except KeyError as error:
                    logging.exception(error)
                
                try:
                    if self.initializedPath_object.prism_smsd_log_path_dict["prism_smsd_tlog_path"]:
                        logging.debug('%s smsd tlog path exists', pname)
                        if tlogProcessor_object.process_tlog_db_enteries("PRISM_SMSD"):
                            pass
                except KeyError as error:
                    logging.exception(error)
            
        
        fileWriter_object = FileWriter(self.outputDirectory_object, self.oarm_uid)
        
        if self.prism_data_dict_list:
            fileWriter_object.write_json_tlog_data(self.prism_data_dict_list)