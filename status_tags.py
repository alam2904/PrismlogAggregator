class logMode(object):
    IS_DEBUG_DATA = "txn"
    IS_DEBUG_ERROR = "error"

#prism tags
class Prism_St_SString(object):
    #initial index search
    search_string = ["-process handler params for task {} for subType:{}","-Handler not found"]

class Prism_En_SString(object):
    #final index search
    search_string = ["-Tlog record added:{}","-Tlog record added:TSK = {}"]
    
class Gs_St_SSring(object):
    #initial index search
    search_string = ["-COUNTRY_CODE", "-In processRequest API of GenericServerProcessor"]

class Gs_En_SSring(object):
    #final index search
    search_string = ["-return fetch size =", "-RESPONSE ="]

class PrismTlogIssueTag(object):
    """
    handler issue tag
    """
    SUB_TYPE_CHECK = ["STCK=3,", "STCK=0,", "STCK=8,"]
    CHECK_BALANCE = ["CBAL=3,", "CBAL=40,", "CBAL=0,", "CBAL=8,", "CBAL=4,"]
    RESERVE = ["RSRV=3,", "RSRV=0,", "RSRV=8,"]
    CHARGING = ["CHG=3,", "CHG=4,", "CHG=0,", "CHG=8,"]
    REMOTE_ACT = ["RMAC=3,", "RMAC=0,", "RMAC=8,"]
    REMOTE_DCT = ["RMD=3,", "RMD=0,", "RMD=8,"]
    DECTIVATION = ["DCT=3,", "DCT=0,", "DCT=8,"]
    CALLBACK = ["CBCK=3,", "CBCK=0,", "CBCK=8,"]
    CDR = ["CRM=3,", "CRM=0,", "CRM=8,"]
    INFORM_CSS = ["CSS=3,", "CSS=0,", "CSS=8,"]
    REFUND = ["RFD=3,", "RFD=31,", "RFD=0,", "RFD=8,"]
    INFORM_OMF = ["OMF=3,", "OMF=0,", "OMF=8,"]
    GENERIC_TASK1 = ["GT1=3,", "GT1=0,", "GT1=8,"]
    GENERIC_TASK2 = ["GT2=3,", "GT2=0,", "GT2=8,"]
    GENERIC_TASK3 = ["GT3=3,", "GT3=0,", "GT3=8,"]
    GENERIC_TASK4 = ["GT1=4,", "GT4=0,", "GT4=8,"]
    GENERAL_FAILURE = ["NHF:NO handler configured for request", "-#TIMEOUT"]

class PrismGeneralIssueTage(object):
    SUB_TYPE_CHECK = "STCK"
    CHECK_BALANCE = "CBAL"
    RESERVE = "RSRV"
    CHARGING = "CHG"
    REMOTE_ACT = "RMAC"
    REMOTE_DCT = "RMD"
    DECTIVATION = "DCT"
    CALLBACK = "CBCK"
    CDR = "CRM"
    INFORM_CSS = "CSS"
    REFUND = "RFD"
    INFORM_OMF = "OMF"
    GENERIC_TASK1 = "GT1"
    GENERIC_TASK2 = "GT2"
    GENERIC_TASK3 = "GT3"
    GENERIC_TASK4 = "GT1" 

class PrismTlogSmsTag(object):
    SMS_INVALID = "I"
    SMS_RETRY_EXCEEDED = "E"
    SMS_PENDING = "P"
    SMS_SUSPENDED = "S"
    SMS_QUEUED = "Q"

class HttpErrorCodes(object):
    #RETRIABLE ERROR (HTTP Status starting with 6**)
    SM_AUTH_FAILED = "600"
    SM_DB_ERROR = "601"
    SM_RETRY_REQUIRED = "602"
    SM_BILLING_IN_PROGRESS = "603"
    SM_SBN_MODE_UNKNOWN = "606"
    SM_PARENT_UNDER_BILLING = "607"
    SM_SBN_DEACT_PENDING = "603"
    SM_UNKNOWN_ERROR = "605"
    SM_REQUEST_LIMIT_EXCEEDED = "608"
    SM_USER_LIMIT_EXCEEDED = "609"
    SM_BAD_REQUEST = "400"
    
    #NON RETRY ERRORS (HTTP Status starting with 7**)
    SM_SUB_NOT_FOUND = "701"
    SM_SUBN_NOT_FOUND = "702"
    SM_SERVICE_NOT_FOUND = "703"
    SM_EVENT_NOT_FOUND = "730"
    SM_REQUEST_INVALID = "704"
    SM_SUBSCRIPTION_ALREADY_EXISTS = "705"
    SM_TRIAL_NOT_ALLOWED = "706"
    SM_MISSING_PARAMETERS = "707"
    SM_MISSING_ARGUMENTS = "707"
    SM_DUPLICAT_REFID = "708"
    SM_EVT_KEY_NOT_FOUND = "709"
    SM_EVT_SBN_REQUIRED = "710"
    SM_EVT_ALREADY_EXISTS = "731"
    SM_PARENT_NOT_ACTIVE = "711"
    SM_TRIGGER_NOT_FOUND = "712"
    SM_TRIGGER_OVERLAPPED = "713"
    SM_LOW_BAL = "714"
    SM_SUB_BLACKLISTED = "715"
    SM_SERVICE_INFO_INVALID = "715"
    SM_UPGRADATION_NOT_FOUND = "716"
    SM_INCORRECT_PARAMETER_LENGTH = "717"
    SM_SUBSCRIPTION_IN_HOLD = "720"
    SM_PARENT_UNDER_SUSPENSION = "721"
    SM_SBN_UNDER_DEACTIVATION = "722"
    SM_SBN_NOT_ACTIVE = "723"
    SM_SBN_ALREADY_IN_SUSPENSION = "724"
    SM_SBN_NOT_IN_SUSPENSION = "725"
    SM_MSISDN_OUTDATED = "726"
    SM_NON_RETRIABLE_UNKNOWN_ERROR = "727"
    SM_NON_RETRIABLE_DCT_ERROR_FROM_CUSTOMER_END = "728"
    SM_SBN_ALREADY_DEACTIVE = "729"
    SM_SBN_NOT_IN_DELAY_DCT = "732"
    SM_VALIDATION_ERROR_BEFORE_ACT = "735"
    
class GsErrorCodes(object):
    """
    Generic Server Constants
    """
    DEFAULT_ERROR_CODE = "-1"
    REQ_MAP_ERROR_CODE = "-2"
    SBN_NOT_AWAITING_CBCK_CODE = "-3"
    RECORD_NOT_UPDATED_CODE = "-4"
    REQ_PARKED = "-5"
    DEFAULT_FAIL_CODE = "0"
    SM_USER_LIMIT_EXCEEDED = "609"
    
class PrismTasks(object):
    """
    Task type class
    """
    SUB_TYPE_CHECK = "S"
    CHECK_BALANCE = "Q"
    RESERVE = "RA"
    CHARGING = "B"
    # CHG = "B"
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
    
class PrismFlowId(object):
    #charge type flow id mapping
    A = ["1", "7"]
    R = ["2", "9", "10"]
    U = ["4"]
    D = ["3"]
    
class PrismFlowType(object):
    #charge type flow type mapping
    GRACE = ["7", "8"]
    SUS = ["10"]
    NULL = ["1", "2", "3"]
    # NO_FLOW = ["1", "2", "3"]

class TimeZoneGmtOffsetValue(object):
    #offset value in UTC
    Africa_Abidjan = "+0000"
    Africa_Accra = "+0000"
    Africa_Lagos = "+0100"
    America_La_Paz = "-0400"
    America_Mexico_City = "-0500"
    America_Nassau = "-0500"
    America_St_Johns = "-0330"
    Asia_Jakarta = "+0700"
    Asia_Kolkata = "+0530"
    Asia_Muscat = "+0400"
    Asia_Tehran = "+0330"
    Europe_Athens = "+0300"
    Pacific_Efate = "+1100"
    Pacific_Marquesas = "-0930"
    Pacific_Pago_Pago = "-1100"

    # Pacific_Pago_Pago =	"-1100"
    # Pacific_Marquesas =	"-0930"
    # America_Nassau = "-0500"
    # America_St_Johns = "-0330"
    # America_Danmarkshavn = ""
    # Asia_Tehran = "+0330"
    # Asia_Kolkata = "+0530"
    # Asia_Jakarta = "+0700"
    # Pacific_Efate = "+1100"
    # Africa_Lagos = "+0100"
    # Europe_Athens = "+0300"
    # Africa_Accra = "+0000"
    # Africa_Abidjan = "+0000"
    # America_Mexico_City = "-0500"
    # Asia_Muscat = "+0400"
    # America_La_Paz = "-0400"

class PrismHandlerClass(object):
    GENERIC_HTTP = "com.onmobile.prism.interfaceHandler.GenericHTTPHandlerV2"
    GENERIC_SOAP = "com.onmobile.prism.interfaceHandler.GenericSoapHandler"
    GENERIC_CDR = "com.onmobile.prism.interfaceHandler.universal.cdr.GenericCDRWriter"
    GENERIC_AUTH = "com.onmobile.prism.interfaceHandler.auth.GenericHTTPAuthenticationHandlerV2"
    