from datetime import datetime

class DateTimeFormatter:
    
    def format_date(self, input_date):
        return datetime.strptime(input_date,"%Y%m%d")
        