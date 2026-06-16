import json
import logging
import os
import requests
from dotenv import load_dotenv

load_dotenv()

_TOKEN_URL = 'https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal'
_MSG_URL   = 'https://open.larksuite.com/open-apis/im/v1/messages'
_APP_ID     = os.environ.get('LARK_APP_ID', '')
_APP_SECRET = os.environ.get('LARK_APP_SECRET', '')
_UNION_ID   = os.environ.get('LARK_UNION_ID', '')

logger = logging.getLogger(__name__)


def _get_token() -> str:
    res = requests.post(_TOKEN_URL, json={'app_id': _APP_ID, 'app_secret': _APP_SECRET})
    data = res.json()
    if data.get('code') != 0:
        raise RuntimeError(f"Lark token error: {data.get('msg')}")
    return data['tenant_access_token']


def notify(message: str):
    if not (_APP_ID and _APP_SECRET and _UNION_ID):
        logger.warning("Lark notifier not configured — skipping notification")
        return
    try:
        token = _get_token()
        res = requests.post(
            _MSG_URL,
            params={'receive_id_type': 'union_id'},
            headers={
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json; charset=utf-8',
            },
            data=json.dumps({
                'receive_id': _UNION_ID,
                'msg_type': 'text',
                'content': json.dumps({'text': message}),
            }),
        )
        data = res.json()
        if data.get('code') != 0:
            logger.error(f"Lark send error: {data.get('msg')}")
    except Exception as e:
        logger.error(f"Lark notify failed: {e}")
