import os
import yaml

CURRENT_WORKING_DIR = os.getcwd()
CONFIG = {
    "PREVIEW_LIMIT_ROWS": 5
}
# Load profiles.yml only once
profiles_path = os.path.join(CURRENT_WORKING_DIR, "profiles.yml")
with open(profiles_path, "r") as file:
    PROFILES = yaml.safe_load(file)