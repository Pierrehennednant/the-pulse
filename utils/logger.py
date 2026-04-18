import logging

class PulseLogger:
    def __init__(self):
        self.logger = logging.getLogger("ThePulse")
        self.logger.setLevel(logging.INFO)

        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)

        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        stream_handler.setFormatter(formatter)

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
