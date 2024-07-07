from settings import settings
from typing import Dict


def construct_base_urls() -> Dict[str, str]:
    """Constructs a dictionary of base URLs for QRadar consoles."""
    return {
        ip: f"https://{ip}"
        for ip in [
            settings.console_1_ip,
            settings.console_2_ip,
            settings.console_3_ip,
            settings.console_aa_ip,
            settings.console_uae_ip,
            settings.console_us_ip,
        ]
    }


# def adjust_stop_time(search_params):
#     large_queries = ["AllowedOutboundTraffic", "AllowedInboundTraffic", "AuthenticationSuccess"]
#     start_time = datetime.strptime(search_params["start_time"], "%Y-%m-%d %H:%M:%S")
#     stop_time = datetime.strptime(search_params["stop_time"], "%Y-%m-%d %H:%M:%S")

#     # Calculate time difference
#     time_diff = stop_time - start_time

#     # Check if query is in allowed list and time difference exceeds 3 hours
#     for query_name in search_params["query"]:
#         if query_name in large_queries and time_diff > timedelta(hours=3):
#             new_stop_time = start_time + timedelta(hours=3)
#             search_params["stop_time"] = new_stop_time.strftime("%Y-%m-%d %H:%M:%S")
#             search_params["query"][query_name] = search_params["query"][query_name].replace(
#                 stop_time.strftime("%Y-%m-%d %H:%M:%S"), new_stop_time.strftime("%Y-%m-%d %H:%M:%S")
#             )
#             break  # No need to check further if adjustment is made

#     return search_params


def get_search_params():
    search_params = {
        "customer_name": "Vermont Information Processing",
        "start_time": "2024-06-29 00:00:00",
        "stop_time": "2024-06-29 03:00:00",
        "event_processor": "119",
    }
    customer_name = search_params["customer_name"]
    start_time = search_params["start_time"]
    stop_time = search_params["stop_time"]
    event_processor = search_params.get("event_processor")
    search_params["query"] = {
        "query_name": "AllowedOutboundTraffic",
        "query_expression": f"SELECT domainId AS 'Domain', DOMAINNAME(domainId) AS domainName, Action, eventdirection AS 'Event Direction', CATEGORYNAME(category) AS 'Low Level Category', QIDNAME(qid)  AS  'Event Name', qid, eventCount  AS  'Event Count', startTime  AS  'Start Time', destinationGeographicLocation AS 'Destination Geographic Country/Region',\"Rule Name\" as 'Rule Name (custom)', \"Bytes Received\" as 'Bytes Received', \"Bytes Sent\" as 'Bytes Sent', Application as 'Application (custom)', \"Policy Name\", Policy FROM events WHERE DOMAINNAME(domainId) = '{customer_name}' AND ((destinationport NOT IN (0, 1, 2, 3, 43, 161, 162) AND ((highlevelcategory = 4000 AND category IN (4002, 4007, 4012, 4016, 4025, 4027, 4031, 4037, 4039))) AND (INCIDR('10.0.0.0/8',sourceip) OR INCIDR('172.16.0.0/12', sourceip) OR INCIDR('192.168.0.0/16', sourceip)) AND NOT (INCIDR('10.0.0.0/8',destinationip) OR INCIDR('172.16.0.0/12', destinationip) OR INCIDR('0.0.0.0/8', destinationip) OR INCIDR('192.168.0.0/16', destinationip) OR INCIDR('169.254.0.0/16', destinationip) OR INCIDR('127.0.0.0/8', destinationip)) AND FULLNETWORKNAME(destinationip, domainId) = 'other' AND NOT referencesetcontains('Known DNS traffic', destinationIP))) START '{start_time}' STOP '{stop_time}' PARAMETERS REMOTESERVERS=ARIELSERVERS4EPID({event_processor})",
    }

    return search_params
