import logging.config
import os
_logger_initialized = False
def setup_logger(source):
    log_dir='spark_manual_logs'
    os.makedirs('spark_manual_logs',exist_ok=True)
    global _logger_initialized
    if not _logger_initialized:
        dict_config = {
                        'version':1,
                        'disable_existing_loggers':False,
                        'formatters':{
                                'standard': {
                                    'format': '%(asctime)s [%(levelname)s] [%(name)s] %(message)s'
                                }
                            },
                        'handlers': {
                            'console': {
                                'class': 'logging.StreamHandler',
                                'formatter': 'standard',
                                'level': 'INFO'
                            },
                            'file': {
                                        'class': 'logging.handlers.RotatingFileHandler',
                                        'formatter': 'standard',
                                        'level': 'INFO',
                                        'filename': f'{log_dir}/{source}.log',
                                        'maxBytes': 2 * 1024 * 1024,
                                        'encoding': 'utf8'
                                    }
                                },
                        'root': {
                            'level':'INFO',
                            'handlers':['console','file']
                        }
                        }
        logging.config.dictConfig(dict_config)
        _logger_initialized = True
    return logging.getLogger(source)



