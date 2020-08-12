import pytz
import os
import logging.config

from utils.env import env
from datetime import datetime

DEBUG = env("DEBUG",False)


HOME_DIR = os.path.dirname(os.path.abspath(__file__))

TIME_ZONE = env("TIME_ZONE",'Australia/Perth')
TZ = datetime.now(tz=pytz.timezone(TIME_ZONE)).tzinfo

START_WORKING_HOUR =  env("START_WORKING_HOUR",vtype=int)
END_WORKING_HOUR =  env("END_WORKING_HOUR",vtype=int)

logging.basicConfig(level="WARNING")

LOG_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'console': {'format':  '%(asctime)s %(levelname)-8s %(name)-15s %(message)s'},
    },
    'handlers': {
        'console': {
            'level': 'DEBUG' if DEBUG else 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'console'
        },
    },
    'loggers': {
        'resource_tracking': {
            'handlers': ['console'],
            'level': 'DEBUG' if DEBUG else 'INFO',
            'propagate':False
        },
        'data_storage': {
            'handlers': ['console'],
            'level': 'DEBUG' if DEBUG else 'INFO',
            'propagate':False
        },
        'db': {
            'handlers': ['console'],
            'level': 'DEBUG' if DEBUG else 'INFO',
            'propagate':False
        },
        'files': {
            'handlers': ['console'],
            'level': 'DEBUG' if DEBUG else 'INFO',
            'propagate':False
        },
        'nginx': {
            'handlers': ['console'],
            'level': 'DEBUG' if DEBUG else 'INFO',
            'propagate':False
        }
    },
    'root':{
        'handlers': ['console'],
        'level': 'WARNING',
        'propagate':False
    }
}
logging.config.dictConfig(LOG_CONFIG)
