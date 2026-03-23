from datetime import datetime
from utils.logger import pulse_logger

class ErrorHandler:
    def __init__(self):
        self.errors = []
    
    def handle(self, exception, pillar_name=None):
        error_entry = {
            'pillar': pillar_name or 'Unknown',
            'error': str(exception),
            'timestamp': datetime.now().isoformat()
        }
        self.errors.append(error_entry)
        pulse_logger.log(f"✗ Error in {pillar_name}: {str(exception)}", level="ERROR")
        return error_entry
    
    def check_stale(self, last_update_time, threshold_minutes):
        from datetime import timedelta
        age = datetime.now() - last_update_time
        return age > timedelta(minutes=threshold_minutes)
    
    def get_stale_warning(self, pillar_name, last_update_time):
        return {
            'status': 'stale',
            'pillar': pillar_name,
            'last_updated': last_update_time.isoformat(),
            'warning': f'⚠️ {pillar_name} data is stale. Displaying last known data.'
        }

error_handler = ErrorHandler()
