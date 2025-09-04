import os
import yaml
import pathlib

DEFAULT_YAML = "etc/biometric-sync.yaml"

def _load_from_yaml(yaml_path=DEFAULT_YAML):
    if not os.path.exists(yaml_path):
        raise FileNotFoundError(yaml_path)
    
    with open(yaml_path, "r") as f:
        cfg = yaml.safe_load(f) or {}
    
    # ===== ERP / Auth =====
    erp = cfg.get("erp", {}) or {}
    url = erp.get("url", "http://localhost:8000")
    api_key = erp.get("api_key")
    api_secret = erp.get("api_secret")
    token_env = erp.get("token_env") # Contoh: "API_KEY:API_SECRET"

    if token_env:
        token = os.getenv(token_env, "")
        if ":" in token:
            api_key, api_secret = token.split(":", 1)
    
    # ===== Ops defaults =====
    pull_freq = int(cfg.get("pull_frequency", 60)) # detik
    logs_dir = cfg.get("logs_dir", "logs")
    import_start_date = cfg.get("import_start_date") #YYYYYMMDD atau None
    erpnext_version = int(cfg.get("erpnext_version", 15))

    # ===== Pastikan direktori log ada =====
    devices_cfg = cfg.get("devices", []) or []
    devices = []
    for d in devices_cfg:
        devices.append({
            "device_id": d.get("name") or d.get("device_id"),
            "ip": d.get("host") or d.get("ip"),
            "clear_from_device_on_fetch": bool(d.get("clear_on_fetch", False)),
            "punch_direction": d.get("punch_direction", "AUTO"),
            "latitude" : d.get("latitude"),
            "longitude" : d.get("longitude")
        })
    
    # ===== Legacy bits (Kompatibilitas) =====
    allowed_exceptions = cfg.get("allowed_exceptionss", [1,2,3])
    shift_map = cfg.get("shift_type_device_mappping", [])

    # ===== Expose sebagai object attribute-style =====
    class _C: pass
    c = _C()
    c.ERPNEXT_URL = url
    c.ERPNEXT_API_KEY = (api_key or "").strip()
    c.ERPNEXT_API_SECRET = (api_secret or "").strip()
    c.ERPNEXT_VERSION = erpnext_version
    
    c.PULL_FREQUENCY = pull_freq
    c.LOGS_DIRECTORY = logs_dir
    c.IMPORT_START_DATE = import_start_date
    
    c.devices = devices
    c.allowed_exception = allowed_exceptions
    c.shift_typ_device_mapping = shift_map
    return c

try:
    config = _load_from_yaml()
except FileNotFoundError:
    import local_config as config