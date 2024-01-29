import os
from dotenv import load_dotenv

BASEDIR = os.path.abspath(os.path.dirname(__file__))

# Connect the path with your '.env' file name
load_dotenv(os.path.join(BASEDIR, ".env"))

DELTA_PATH = os.environ.get("DELTA_PATH")
SOURCE_PATH = os.environ.get("SOURCE_PATH")



