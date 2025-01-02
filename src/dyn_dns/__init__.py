import logging

from pydantic import BaseModel, ValidationError

from database.database import BackendDatabase


# Model to store the dyn dns details
class DynDnsDetails(BaseModel):
    synology_username: str
    synology_password: str
    current_external_ip: str
    cloudflare_api_base_url: str
    cloudflare_zone_id: str
    cloudflare_dns_record_id: str
    cloudflare_api_token: str


SYNOLOGY_API_BASE_URL = "http://192.168.1.1:8000/webapi"
SYNOLOGY_AUTH_API = "SYNO.API.Auth"
SYNOLOGY_AUTH_URL = "/auth.cgi"
SYNOLOGY_AUTH_VERSION = 3
SYNOLOGY_AUTH_LOGIN_METHOD = "login"
SYNOLOGY_AUTH_LOGOUT_METHOD = "logout"

SYNOLOGY_GET_EXTERNAL_IP_API = "SYNO.Core.DDNS.ExtIP"
SYNOLOGY_GET_EXTERNAL_IP_URL = "/entry.cgi"
SYNOLOGY_GET_EXTERNAL_IP_VERSION = 1
SYNOLOGY_GET_EXTERNAL_IP_METHOD = "list"


# Get a backend database instance
database = BackendDatabase()

# Set the database in use
database.set_database("web_database")

# Set the collection in use
DNS_INFO_COLLECTION = database.get_collection("dns_info")

if DNS_INFO_COLLECTION is not None:
    try:
        # Get the dyn dns details from the database
        dns_info = DNS_INFO_COLLECTION.find_one({})

        # If the dns_info is not None, create a DynDnsDetails instance
        if dns_info is not None:
            # Create a DynDnsDetails instance
            dyn_dns_details = DynDnsDetails(**dns_info)
        else:
            # If the dns_info is None, log an error and exit
            logging.error("Failed to get the dyn dns details from the database.")
            dyn_dns_details = None
    except ValidationError as e:
        # If the validation fails, log the error and exit
        logging.error(
            f"Failed to get the dyn dns details from the database. Error: {e}"
        )
        dyn_dns_details = None
else:
    # If the collection is None, log an error and exit
    logging.error("Failed to get the collection from the database.")
    dyn_dns_details = None

if dyn_dns_details is None:
    CLOUDFLARE_DNS_UPDATE_URL = None
    CLOUDFLARE_HEADERS = None
else:
    # Set the cloudflare URL to update the DNS record
    CLOUDFLARE_DNS_UPDATE_URL = f"{dyn_dns_details.cloudflare_api_base_url}/zones/{dyn_dns_details.cloudflare_zone_id}/dns_records/{dyn_dns_details.cloudflare_dns_record_id}"

    # Set the headers to include the api key
    CLOUDFLARE_HEADERS = {
        "Content-Type": "application/json",
        "Authorization": f"bearer {dyn_dns_details.cloudflare_api_token}",
    }

# Get the Notify Run Endpoint
try:
    with open(
        "src/secrets/notify_run_endpoint.txt", "r", encoding="utf-8"
    ) as notify_run_endpoint_file:
        NOTIFY_RUN_ENDPOINT = notify_run_endpoint_file.read().strip()
except FileNotFoundError:
    # If the file is not found, log an error and set the NOTIFY_RUN_ENDPOINT to None
    logging.error("Failed to get the notify run endpoint. File not found.")
    NOTIFY_RUN_ENDPOINT = None

from .dyn_dns import dyn_dns_loop

# Export the dyn_dns_loop function
__all__ = [
    "dyn_dns_loop",
]
