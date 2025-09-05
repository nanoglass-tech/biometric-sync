from __future__ import annotations
import os
from types import SimpleNamespace as NS
from pathlib import Path

try:
    from dotenv import load_dotenv
except:
    def load_dotenv(*a, **k):
        return

try:
    import yaml
except Exception:
    yaml = None

def _ns(d: dict) -> NS:
    return NS(**d)

def load() -> NS:
    """
    Urutan load:
    1) .env (untuk secret & flag)
    2) YAM (default /etc/biometric-sync.yaml atau $BIOSYNC_CONFIG)
    3)fallback ke local_config.py (backward compatible)
    """
    load_dotenv()
    
    # path YAML: env override -> default /etc
    cfg_path = os.getenv("BIOSYNC_CONFIG", "/etc/biometric-sync.yaml")
    cfg_file = Path(cfg_path)

    if yaml and cfg_file.exists():
        data = yaml.safe_load(cfg_file.read_text()) or {}
        erp = data.get("erp", {})
        ops = data.get("ops", {})
        devices = data.get("devices", [])
        cfg = {
            # ERP & secret dari ENV override bila ada
            "ERPNEXT_URL": erp.get("url", "http://localhost:8000"),
            "ERPNEXT_API_KEY": os.getenv("ERP_API_KEY", erp.get("api_key", "")),
            "ERPNEXT_API_SECRET": os.getenv("ERP_API_SECRET", erp.get("api_secret", "")),
            "ERPNEXT_VERSION": erp.get("version", 14),
            
            # Ops
            "PULL_FREQUENCY": ops.get("pull_frequency", 60),
            "LOGS_DIRECTORY": ops.get("logs_dir", "logs"),
            "IMPORT_START_DATE": ops.get("import_start_date"),
            
            # Devices & mapping
            "devices": devices,
            "shift_type_device_mapping": data.get("shift_type_device_mapping", []),
            
            # ERP error allowlist (default sama seperti upstream)
            "allowed_exceptions": data.get("allowed_exceptions", [1,2,3])
        }
        return _ns(cfg)

    # fallback
    import local_config as config
    return config