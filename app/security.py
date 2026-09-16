import hmac
import hashlib
import json

def generate_signature(secret: str, payload: dict, timestamp: int) -> str:
    """Signs payload and timestamp using HMAC-SHA256."""
    canonical_payload = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    signed_content = f"{timestamp}.{canonical_payload}".encode("utf-8")
    signature = hmac.new(secret.encode("utf-8"), signed_content, hashlib.sha256).hexdigest()
    return f"t={timestamp},v1={signature}"