# from tlog import Tlog
from enum import Enum

class TlogErrorTag(Enum):
    """
    Enum error tag
    """
    SUB_TYPE_CHECK = "STCK=3"
    CHECK_BALANCE = "CBAL=3"
    RESERVE = "RSRV=3"
    CHARGING = "CHG=3"
    REMOTE_ACT = "RMAC=3"
    REMOTE_DCT = "RMD=3"
    DECTIVATION = "DCT=3"
    CALLBACK = "CBCK=3"
    CDR = "CRM=3"
    INFORM_CSS = "CSS=3"
    REFUND = "RFD=3"
    INFORM_OMF = "OMF=3"
    GENERIC_TASK1 = "GT1=3"
    GENERIC_TASK2 = "GT2=3"
    GENERIC_TASK3 = "GT3=3"
    GENERIC_TASK4 = "GT4=3"
    GENERIC_TASK5 = "GT5=3"


class TlogRetryTag(Enum):
    """
    Enum retry tag
    """
    SUB_TYPE_CHECK = "STCK=0"
    CHECK_BALANCE = "CBAL=0"
    RESERVE = "RSRV=0"
    CHARGING = "CHG=0"
    REMOTE_ACT = "RMAC=0"
    REMOTE_DCT = "RMD=0"
    DECTIVATION = "DCT=0"
    CALLBACK = "CBCK=0"
    CDR = "CRM=0"
    INFORM_CSS = "CSS=0"
    REFUND = "RFD=0"
    INFORM_OMF = "OMF=0"
    GENERIC_TASK1 = "GT1=0"
    GENERIC_TASK2 = "GT2=0"
    GENERIC_TASK3 = "GT3=0"
    GENERIC_TASK4 = "GT4=0"
    GENERIC_TASK5 = "GT5=0"
    
    
class TlogLowBalTag(Enum):
    """
    Enum low bal tag
    """
    CHECK_BALANCE = "CBAL=4"
    CHARGING = "CHG=4"

class TlogNHFTag(Enum):
    """
    Enum no handler found tag
    """
    NHF = "NHF:NO handler configured for request"

class TlogAwaitPushTag(Enum):
    """
    Enum await push tag
    """
    SUB_TYPE_CHECK = "STCK=7"
    CHECK_BALANCE = "CBAL=7"
    RESERVE = "RSRV=7"
    CHARGING = "CHG=7"
    REMOTE_ACT = "RMAC=7"
    REMOTE_DCT = "RMD=7"
    DECTIVATION = "DCT=7"
    CALLBACK = "CBCK=7"
    CDR = "CRM=7"
    INFORM_CSS = "CSS=7"
    REFUND = "RFD=7"
    INFORM_OMF = "OMF=7"
    GENERIC_TASK1 = "GT1=7"
    GENERIC_TASK2 = "GT2=7"
    GENERIC_TASK3 = "GT3=7"
    GENERIC_TASK4 = "GT4=7"
    GENERIC_TASK5 = "GT5=7"
    
class TlogAwaitPushTimeOutTag(Enum):
    """
    Enum timeout tag
    """
    SUB_TYPE_CHECK = "STCK=96,-,,,-#TIMEOUT"
    CHECK_BALANCE = "CBAL=96,-,,,-#TIMEOUT"
    RESERVE = "RSRV=96,-,,,-#TIMEOUT"
    CHARGING = "CHG=96,-,,,-#TIMEOUT"
    REMOTE_ACT = "RMAC=96,-,,,-#TIMEOUT"
    REMOTE_DCT = "RMD=96,-,,,-#TIMEOUT"
    DECTIVATION = "DCT=96,-,,,-#TIMEOUT"
    CALLBACK = "CBCK=96,-,,,-#TIMEOUT"
    CDR = "CRM=96,-,,,-#TIMEOUT"
    INFORM_CSS = "CSS=96,-,,,-#TIMEOUT"
    REFUND = "RFD=96,-,,,-#TIMEOUT"
    INFORM_OMF = "OMF=96,-,,,-#TIMEOUT"
    GENERIC_TASK1 = "GT1=96,-,,,-#TIMEOUT"
    GENERIC_TASK2 = "GT2=96,-,,,-#TIMEOUT"
    GENERIC_TASK3 = "GT3=96,-,,,-#TIMEOUT"
    GENERIC_TASK4 = "GT4=96,-,,,-#TIMEOUT"
    GENERIC_TASK5 = "GT5=96,-,,,-#TIMEOUT"
    
class TlogHandlerExp(Enum):
    CHARGING = "CHG=30"
    CHG = "CHG=41"
    
class TaskType(Enum):
    """
    Task type class
    """
    SUB_TYPE_CHECK = "S"
    CHECK_BALANCE = "Q"
    RESERVE = "RA"
    CHARGING = "B"
    CHG = "B"
    REMOTE_ACT = "R"
    REMOTE_DCT = "H"
    DECTIVATION = "D"
    CALLBACK = "C"
    CDR = "L"
    INFORM_CSS = "M"
    REFUND = "W"
    INFORM_OMF = "IO"
    GENERIC_TASK1 = "G1"
    GENERIC_TASK2 = "G2"
    GENERIC_TASK3 = "G3"
    GENERIC_TASK4 = "G4"
    GENERIC_TASK5 = "G5"

# for status in TlogErrorTag:
#     print(status.value)
