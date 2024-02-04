import sys
import json
from pathlib import Path

from pydantic import BaseModel, ValidationError

# Model to store the dyn dns details
class DynDnsDetails(BaseModel):
    get_external_ip_url: str
    current_external_ip: str
    cloudflare_api_base_url: str
    cloudflare_zone_id: str
    cloudflare_dns_record_id: str
    cloudflare_api_token: str

# Path to the dyn dns details file
DYN_DNS_FILE = Path('src/dyn_dns/dyn_dns.json')

# Try to read the dyn dns details from the file
try:
    with open(DYN_DNS_FILE, 'r', encoding='utf-8') as dyn_dns_file:
        # If the file is valid, store the details
        dyn_dns_details = DynDnsDetails.model_validate(json.load(dyn_dns_file))
except FileNotFoundError:
    # If this fails there's nothing we can do, so exit
    print('No dyn_dns.json file found')
    sys.exit()
except ValidationError as e:
    # If the file is invalid, print the error and exit
    print(f'Invalid dyn_dns.json file: {e}')
    sys.exit()

# Get the Notify Run Endpoint
try:
    with open('notify_run_endpoint.txt', 'r', encoding='utf-8') as notify_run_endpoint_file:
        NOTIFY_RUN_ENDPOINT = notify_run_endpoint_file.read().strip()
except FileNotFoundError:
    NOTIFY_RUN_ENDPOINT = None

# Set the cloudflare URL to update the DNS record
CLOUDFLARE_DNS_UPDATE_URL = f'{dyn_dns_details.cloudflare_api_base_url}/zones/{dyn_dns_details.cloudflare_zone_id}/dns_records/{dyn_dns_details.cloudflare_dns_record_id}'

# Set the headers to include the api key
CLOUDFLARE_HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"bearer {dyn_dns_details.cloudflare_api_token}",
}

from .dyn_dns import dyn_dns_loop

# Export the dyn_dns_loop function
__all__ = [
    'dyn_dns_loop',
]
