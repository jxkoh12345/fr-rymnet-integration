import os
import logging
import requests
from dotenv import load_dotenv
# Context: Experimenting on accessing the device straight. The initial project we are accessing the data from one of their server which the device send its data to. Which we assume that the data in that server is correct (This is false btw).

load_dotenv()
username = os.getenv('ISAPI_USERNAME')
password = os.getenv('ISAPI_PASSWORD')

logging.basicConfig(
    filename='device_event.log',
    level=logging.INFO,
    format='%(asctime)s %(message)s',
)
logger = logging.getLogger(__name__)

request_url = 'http://10.1.250.62:80/ISAPI/Event/notification/alertStream'
auth = requests.auth.HTTPDigestAuth(username, password)

# Long-poll: device keeps connection open, pushes multipart/mixed chunks per event
with requests.get(request_url, auth=auth, stream=True, timeout=None) as response:
    logger.info('status %s', response.status_code)
    for chunk in response.iter_lines(chunk_size=1024):
        if chunk:
            logger.info(chunk.decode(errors='replace'))