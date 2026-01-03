import os
import json
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv

def fetch_secrets() -> Dict[str, Any]:
    """Returns a JSON object containing user-specific Strava secrets stored locally."""
    
    load_dotenv()
    strava_secrets_path = os.getenv("strava_secrets_path")
    current_dir = Path(__file__).resolve().parent
    parent_dir = current_dir.parent

    with open(f"{parent_dir}/{strava_secrets_path}") as secrets:
        return json.load(secrets)