"""Application configuration — loads env vars and constants."""

import os
import sys
from dotenv import load_dotenv

load_dotenv()

# ── Azure DevOps / TFS ──
AZURE_DEVOPS_ORG_URL = os.environ.get("AZURE_DEVOPS_ORG_URL")
AZURE_DEVOPS_PAT = os.environ.get("AZURE_DEVOPS_PAT")
AZURE_DEVOPS_PROJECT = os.environ.get("AZURE_DEVOPS_PROJECT")

if not all([AZURE_DEVOPS_ORG_URL, AZURE_DEVOPS_PAT, AZURE_DEVOPS_PROJECT]):
    print("Error: Missing Azure DevOps environment variables.")
    sys.exit(1)

# ── OpenAI ──
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

# ── Flask ──
FLASK_SECRET_KEY = os.environ.get("FLASK_SECRET_KEY", os.urandom(24).hex())

# ── Constants ──
HOURS_PER_DAY = 8
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEAM_FILE = os.path.join(BASE_DIR, "team_members.json")
CHAT_HISTORY_FILE = os.path.join(BASE_DIR, "chat_history.json")
APP_CONFIG_FILE = os.path.join(BASE_DIR, "app_config.json")
DAILY_STATUS_FILE = os.path.join(BASE_DIR, "daily_status_history.json")
