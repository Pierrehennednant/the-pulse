from datetime import datetime, timezone
from utils.logger import pulse_logger

class ErrorHandler:
    def __init__(self):
        self.errors = []

    def handle(self, exception, pillar_name=None):
        error_entry = {
            'pillar': pillar_name or 'Unknown',
            'error': str(exception),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        self.errors.append(error_entry)
        self.errors = self.errors[-50:]
        pulse_logger.log(f"✗ Error in {pillar_name}: {str(exception)}", level="ERROR")
        return error_entry

error_handler = ErrorHandler()
