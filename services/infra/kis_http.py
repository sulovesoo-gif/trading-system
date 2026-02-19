import requests

def common_headers(auth, tr_id: str) -> dict:
    return {
        "Content-Type": "application/json",
        "authorization": f"Bearer {auth.get_access_token()}",
        "appkey": auth.api_key,
        "appsecret": auth.api_secret,
        "tr_id": tr_id,
        "custtype": "P",
    }

def kis_get(base_url: str, path: str, headers: dict, params: dict | None = None, timeout: int = 5):
    url = f"{base_url}{path}"
    return requests.get(url, headers=headers, params=params, timeout=timeout)
