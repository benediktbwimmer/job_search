import re
import secrets


_USER_ID_PATTERN = re.compile(r"^[a-zA-Z0-9._-]{1,64}$")


def generate_api_key(prefix: str = "js") -> str:
    clean_prefix = re.sub(r"[^a-zA-Z0-9_-]+", "", str(prefix or "js")).strip("_-")
    if not clean_prefix:
        clean_prefix = "js"
    return f"{clean_prefix}_{secrets.token_urlsafe(24)}"


def normalize_auth_config(auth_cfg: dict | None) -> dict:
    src = auth_cfg if isinstance(auth_cfg, dict) else {}
    enabled = bool(src.get("enabled", False))
    api_keys_src = src.get("api_keys", {})
    api_keys = {}
    if isinstance(api_keys_src, dict):
        for token, user_id in api_keys_src.items():
            t = str(token or "").strip()
            u = str(user_id or "").strip()
            if t and u:
                api_keys[t] = u
    return {"enabled": enabled, "api_keys": api_keys}


def validate_auth_config(auth_cfg: dict | None) -> list[str]:
    errors = []
    if not isinstance(auth_cfg, dict):
        return ["auth config must be a JSON object"]

    if "enabled" in auth_cfg and not isinstance(auth_cfg.get("enabled"), bool):
        errors.append("`enabled` must be a boolean")

    api_keys = auth_cfg.get("api_keys", {})
    if not isinstance(api_keys, dict):
        errors.append("`api_keys` must be an object mapping token -> user_id")
        return errors

    for token, user_id in api_keys.items():
        token_s = str(token or "").strip()
        user_s = str(user_id or "").strip()
        if not token_s:
            errors.append("api key token cannot be empty")
            continue
        if len(token_s) < 12:
            errors.append(f"api key token too short: {token_s!r}")
        if any(ch.isspace() for ch in token_s):
            errors.append(f"api key token contains whitespace: {token_s!r}")

        if not user_s:
            errors.append(f"user_id for token {token_s!r} cannot be empty")
            continue
        if not _USER_ID_PATTERN.match(user_s):
            errors.append(
                f"user_id {user_s!r} invalid (use 1-64 chars: letters, digits, dot, underscore, hyphen)"
            )

    if bool(auth_cfg.get("enabled", False)) and not api_keys:
        errors.append("auth is enabled but `api_keys` is empty")

    return errors
