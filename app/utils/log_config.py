from logging import basicConfig, getLogger, ERROR, DEBUG, INFO, FileHandler, StreamHandler
import os
import datetime
import pytz

def get_logger():

    # Absolute path
    current_script_path = os.path.abspath(__file__)

    # App path
    app_directory = os.path.dirname(os.path.dirname(current_script_path))
    log_path = os.path.join(app_directory, 'data', 'logs')

    # Path dos logs
    os.makedirs(log_path, exist_ok  = True)
    data = str(datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond=0))
    log_file_path = os.path.join(log_path, f'logs-{data}.log')

    # LOG CONFIGS
    basicConfig(
        level = ERROR,
        encoding = 'utf-8',
        format = '[%(asctime)s] - %(levelname)s - %(funcName)s - %(message)s',
        datefmt = '%Y-%m-%d %H:%M:%S'
        # handlers = [FileHandler(log_file_path, 'a', encoding='utf-8'), StreamHandler()]
    )

    return getLogger(__name__)