{

    "Windows_Log_Validation":"SELECT DOMAINNAME(domainId) AS domainName, domainId AS 'Domain', \"Windows_Event_Category\" AS 'Windows_Event_Category', logsourcename(logSourceId) AS 'Log Source', \"deviceType\" AS 'Log Source Type', DATEFORMAT(starttime,'yyyy-MM-dd hh:mm:ss.SSS a Z') AS 'Start Time' from events where DOMAINNAME(domainId) = '{customer_name}' AND deviceType='12' START '{start_time}' STOP '{stop_time}' PARAMETERS REMOTESERVERS=ARIELSERVERS4EPNAME(PROCESSORNAME({event_processor}))"

}