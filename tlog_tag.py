# from tlog import Tlog
from enum import Enum

class TlogErrorTag(Enum):
    """
    Enum error tag class
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
    Enum low bal tag class
    """
    CHECK_BALANCE = "CBAL=4"
    CHARGING = "CHG=4"

class TaskType(Enum):
    """
    Task type class
    """
    SUB_TYPE_CHECK = "S"
    CHECK_BALANCE = "Q"
    RESERVE = "RSV"
    CHARGING = "B"
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
