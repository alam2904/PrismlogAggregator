import logging
from path_initializer import LogPathFinder
from log_processor import PROCESSOR


class Initializer:
    def __init__(self, hostname, outputDirectory_object, config, validation_object, log_mode, uid):
        self.hostname = hostname
        self.outputDirectory_object = outputDirectory_object
        self.config =  config
        self.validation_object = validation_object
        self.log_mode = log_mode
        self.oarm_uid = uid
    
    def initialize_process(self):
        initializedPath_object = LogPathFinder(self.hostname, self.config, self.validation_object)
        try:
            for i in self.config[self.hostname]["PRISM"]:
                logging.info('iorder: %s', i)
                
                try:
                    # if self.config[self.hostname]["PRISM"]:
                    # if self.config[self.hostname]["PRISM"]["PRISM_TOMCAT"] != "":
                    if i == "PRISM_TOMCAT":
                        try:
                            initializedPath_object.initialize_path("PRISM_TOMCAT")
                            formatter = "#" * 100
                            logging.info('\n')
                            logging.info('PRISM TOMCAT PATH INITIALIZED \n %s', formatter)
                            for key, value in initializedPath_object.prism_tomcat_log_path_dict.items():
                                logging.info('%s : %s', key, value)
                            logging.info('\n')    
                        except ValueError as error:
                            logging.warning('Prism tomcat path not initialized. %s', error)
                        except Exception as error:
                            logging.warning(error)
                    
                    # if self.config[self.hostname]["PRISM"]["PRISM_DEAMON"]["PRISM_DEAMON"] != "":
                    elif i == "PRISM_DEAMON":
                        try:
                            initializedPath_object.initialize_path("PRISM_DEAMON")
                            formatter = "#" * 100
                            logging.info('\n')
                            logging.info('PRISM DEAMON PATH INITIALIZED \n %s', formatter)
                            for key, value in initializedPath_object.prism_daemon_log_path_dict.items():
                                logging.info('%s : %s', key, value)
                            logging.info('\n')
                        except ValueError as error:
                            logging.warning('PRISM DEAMON path not initialized. %s', error)
                        except Exception as error:
                            logging.warning(error)

                    # if self.config[self.hostname]["PRISM"]["PRISM_SMSD"]["PRISM_SMSD"] != "":
                    elif i == "PRISM_SMSD":
                        try:
                            initializedPath_object.initialize_path("PRISM_SMSD")
                            formatter = "#" * 100
                            logging.info('\n')
                            logging.info('PRISM SMSD PATH INITIALIZED \n %s', formatter)
                            # logging.info('%s', formatter)
                            for key, value in initializedPath_object.prism_smsd_log_path_dict.items():
                                logging.info('%s : %s', key, value)
                            logging.info('\n')
                        except ValueError as error:
                            logging.warning('PRISM SMSD path not initialized. %s', error)
                        except Exception as error:
                            logging.warning(error)
                    else:
                        logging.error('PRISM %s data not present in %s.json file', i, self.hostname)
                except KeyError as error:
                    logging.info('\n')
                    logging.info('PRISM process not present in %s.json file, hence processing would not be done for PRISM', self.hostname)
                except ValueError as error:
                    logging.warning('any of the %s path not initialized', i)
                except Exception as error:
                    logging.warning(error)
                    
            logging.info('\n')
            logging.info('log mode: %s', self.log_mode)
            
            #processor is called
            processor_object = PROCESSOR(initializedPath_object, self.outputDirectory_object, self.validation_object, self.log_mode, self.oarm_uid, self.config)
            processor_object.process()
                
        except KeyError as error:
            logging.exception(error)