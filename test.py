import requests

if __name__ == "__main__":
    query_status_response_header = {}
    session = requests.Session()
    base_url = "https://192.168.168.97"
    current_record_count = 0
    query_status_response_header["cursor_id"] = "fe664165-3229-4e48-b637-21f538fc2f46"
    query_status_response_header["record_count"] = 101365
    try:
        response = session.get(
            url=f"{base_url}/api/ariel/searches/{query_status_response_header['cursor_id']}/results",
            headers={
                "SEC": "f9c4c5cf-cef3-447e-ad58-e9028fa25658",
                "Range": f"items={current_record_count}-{query_status_response_header['record_count']}",
            },
            stream=True,
        )
        for line in response.iter_lines():
            print(line)
        response.raise_for_status()
    except Exception as e:
        print(e)
