from os import environ
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Bitbucket settings
BITBUCKET_REPOS = environ.get("BITBUCKET_REPOS").split(",")
BITBUCKET_USERNAME = environ.get("BITBUCKET_USERNAME")
BITBUCKET_APP_PASSWORD = environ.get("BITBUCKET_APP_PASSWORD")
BITBUCKET_API_BASE_URL = environ.get("BITBUCKET_API_BASE_URL")

# Database settings
DB_HOST = environ.get("DB_HOST")
DB_PORT = environ.get("DB_PORT")
DB_USER = environ.get("DB_USER")
DB_PSWD = environ.get("DB_PSWD")
DB_NAME = environ.get("DB_NAME")
DB_CHUNK_SIZE = int(environ.get("DB_CHUNK_SIZE"))
