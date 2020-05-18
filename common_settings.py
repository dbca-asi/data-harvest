import pytz
import logging.config

from utils.env import env
from datetime import datetime

DEBUG = env("DEBUG",False)
TIME_ZONE = env("TIME_ZONE",'Australia/Perth')
TZ = datetime.now(tz=pytz.timezone(TIME_ZONE)).tzinfo

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
