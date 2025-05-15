import os
import json
from dotenv import load_dotenv

load_dotenv()

def config(path="/Users/sandeepmalkanoor/Documents/Python/Night_Audit_DataEngineering_Project/night_audit_etl_pipeline/config.json"):
    with open(path) as f:
        raw_config = json.load(f)

    def env_substitute(value):
        if isinstance(value, str):
            return os.path.expandvars(value)
        elif isinstance(value, dict):
            return {k: env_substitute(v) for k, v in value.items()}
        return value

    return env_substitute(raw_config)

log_file_path = os.getenv("LOG_FILE_PATH", config().get("log_file"))
