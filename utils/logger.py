import sys
import re
from datetime import date
from loguru import logger

def logging_setup():

    #format_info = "<green>{time:HH:mm:ss.SS}</green> | <level>{message}</level>"
    format_info = "<green>{time:HH:mm:ss.SS}</green> | <blue>{level:<8}</blue> | <level>{message}</level>"
    format_info_logfile = "{time:HH:mm:ss.SS} | {level:<8} | {name}:{function}:{line:<8} | {message}"
    #format_warning = "<green>{time:HH:mm:ss.SS}</green> | <yellow>{level}</yellow> | <level>{message}</level>"
    format_error = "<green>{time:HH:mm:ss.SS}</green> |<blue>{level}</blue> | " \
                   "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> |<level>{message}</level>"
    file_path = r"logs/"

    logger.remove()

    logger.add(file_path + f"out_{date.today().strftime('%m-%d')}.log", colorize=True,
               format=format_info_logfile)
    logger.add(sys.stdout, colorize=True, format=format_info, level="INFO")
    #logger.add(sys.stdout, colorize=True, format=format_warning, level="WARNING")

def clean_brackets(raw_str):
    clean_text = re.sub(brackets_regex, '', raw_str)
    return clean_text

brackets_regex = re.compile(r'<.*?>')

logging_setup()
