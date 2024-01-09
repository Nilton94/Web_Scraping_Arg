from logging import basicConfig, getLogger, DEBUG, FileHandler, StreamHandler
import os

def get_logger():

    # Absolute path
    current_script_path = os.path.abspath(__file__)

    # App path
    app_directory = os.path.dirname(os.path.dirname(current_script_path))
    log_path = os.path.join(app_directory, 'data', 'logs')

    # Path dos logs
    os.makedirs(log_path, exist_ok  = True)
    log_file_path = os.path.join(log_path, 'logs.log')

    # LOG CONFIGS
    basicConfig(
        level = DEBUG,
        encoding = 'utf-8',
        format = '[%(asctime)s] - %(levelname)s - %(funcName)s - %(message)s',
        datefmt = '%Y-%m-%d %H:%M:%S',
        handlers = [FileHandler(log_file_path, 'a', encoding='utf-8'), StreamHandler()]
    )

    return getLogger(__name__)