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
            for i in self.config[self.hostname]:
                logging.info('iorder: %s', i)
                
                try:
                    if self.config[self.hostname]["PRISM"] and i == 'PRISM':
                        if self.config[self.hostname]["PRISM"]["PRISM_TOMCAT"] != "":
                            try:
                                initializedPath_object.initialize_path(i)
                                formatter = "#" * 100
                                logging.info('\n')
                                logging.info('%s TOMCAT PATH INITIALIZED \n %s', i, formatter)
                                for key, value in initializedPath_object.prism_tomcat_log_path_dict.items():
                                    logging.info('%s : %s', key, value)
                            except ValueError as error:
                                logging.warning('%s Tomcat path not initialized. %s', i, error)
                            except Exception as error:
                                logging.warning(error)
                        else:
                            logging.error(
                                            '%s TOMCAT data not present in %s file,\
                                            Hence PRISM_TOMCAT logs will not be initialized and fetched', i, self.hostname
                                        )
                        
                        if self.config[self.hostname]["PRISM"]["PRISM_DEAMON"] != "":
                            try:
                                initializedPath_object.initialize_path(i)
                                formatter = "#" * 100
                                logging.info('\n')
                                logging.info('%s DAEMON PATH INITIALIZED \n %s', i, formatter)
                                for key, value in initializedPath_object.prism_daemon_log_path_dict.items():
                                    logging.info('%s : %s', key, value)
                            except ValueError as error:
                                logging.warning('%s path not initialized. %s', i, error)
                            except Exception as error:
                                logging.warning(error)
                        else:
                            logging.error(
                                            '%s DEAMON data not present in %s file,\
                                            Hence PRISM_DEAMON logs will not be initialized and fetched', i, self.hostname
                                        )

                        if self.config[self.hostname]["PRISM"]["PRISM_SMSD"] != "":
                            try:
                                initializedPath_object.initialize_path(i)
                                formatter = "#" * 100
                                logging.info('\n')
                                logging.info('%s SMS PATH INITIALIZED \n %s', i, formatter)
                                # logging.info('%s', formatter)
                                for key, value in initializedPath_object.prism_smsd_log_path_dict.items():
                                    logging.info('%s : %s', key, value)
                            except ValueError as error:
                                logging.warning('%s path not initialized. %s', i, error)
                            except Exception as error:
                                logging.warning(error)
                        else:
                            logging.error(
                                            '%s SMSD data not present in %s file,\
                                            Hence PRISM_SMSD logs will not be initialized and fetched', i, self.hostname
                                        )
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