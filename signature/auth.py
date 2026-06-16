import hmac
import hashlib
import base64
import time
import uuid


def build_headers(app_key: str, app_secret: str, path: str, body: str) -> dict:
    timestamp = str(int(time.time() * 1000))
    nonce = str(uuid.uuid4()).replace('-', '')

    content_md5 = base64.b64encode(
        hashlib.md5(body.encode('utf-8')).digest()
    ).decode('utf-8')

    string_to_sign = '\n'.join([
        'POST',
        '*/*',
        content_md5,
        'application/json',
        f'x-ca-key:{app_key}',
        f'x-ca-nonce:{nonce}',
        f'x-ca-timestamp:{timestamp}',
        path
    ])

    signature = base64.b64encode(
        hmac.new(
            app_secret.encode('utf-8'),
            string_to_sign.encode('utf-8'),
            hashlib.sha256
        ).digest()
    ).decode('utf-8')

    return {
        'Content-Type': 'application/json',
        'Accept': '*/*',
        'Content-MD5': content_md5,
        'x-ca-key': app_key,
        'x-ca-timestamp': timestamp,
        'x-ca-nonce': nonce,
        'x-ca-signature-headers': 'x-ca-key,x-ca-nonce,x-ca-timestamp',
        'x-ca-signature': signature,
    }
