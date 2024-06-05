def get_auth_headers(api_secret_key):
    """Constructs headers with accept and x-api-key fields"""
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "Authorization": api_secret_key
    }
    return headers