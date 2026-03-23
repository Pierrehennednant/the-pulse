import logging
from datetime import datetime

class PulseLogger:
    def __init__(self, log_file="the_pulse.log"):
        self.logger = logging.getLogger("ThePulse")
        self.logger.setLevel(logging.INFO)
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)
        
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        stream_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(stream_handler)
    
    def log(self, message, level="INFO"):
        if level == "INFO":
            self.logger.info(message)
        elif level == "ERROR":
            self.logger.error(message)
        elif level == "WARNING":
            self.logger.warning(message)
        elif level == "DEBUG":
            self.logger.debug(message)

pulse_logger = PulseLogger()
