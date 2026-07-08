import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()
URL = os.environ['RYMNET_EMPLOYEE_URL']
TOKEN = os.environ['RYMNET_TOKEN']


def employee_exists(employee_no: str) -> bool:
    """Check Rymnet's employee biodata for employee_no. True if a matching record exists."""
    params = {
        'access_token': TOKEN,
        'format': 'Json',
        'filters': json.dumps({'employee_no': employee_no}),
    }
    res = requests.get(URL, params=params)
    res.raise_for_status()
    data = res.json()
    # found -> [{"employee": [...]}], not found -> {"employee": []} (API is inconsistent)
    if isinstance(data, list):
        data = data[0] if data else {}
    return bool(data.get('employee'))


if __name__ == '__main__':
    import sys
    emp = sys.argv[1] if len(sys.argv) > 1 else 'RC14405'
    print(employee_exists(emp))
