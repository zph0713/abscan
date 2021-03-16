import os
from datetime import datetime
from log import Log

def Logger(logname):
    base_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    log_path = os.path.join(base_path, "logs")

    log_time = datetime.today().strftime('%Y-%m-%d')
    log_file = os.path.join(log_path, logname+"-" + str(log_time) + ".log")
    if not os.path.isdir(log_path):
        os.makedirs(log_path)

    logger_instance = Log(logname, console=1, logfile=log_file, show_details=True)
    return logger_instance
