import os
import json
from dotenv import load_dotenv

load_dotenv()

def config(path="/Users/sandeepmalkanoor/Documents/Python/Night_Audit_DataEngineering_Project/config.json"):
    with open(path) as f:
        raw_config = json.load(f)

    def env_substitute(value):
        if isinstance(value, str):
            return os.path.expandvars(value)
        elif isinstance(value, dict):
            return {k: env_substitute(v) for k, v in value.items()}
        return value

    return env_substitute(raw_config)
