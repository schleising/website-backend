from datetime import datetime, timedelta, timezone
import logging
from threading import Event
from zoneinfo import ZoneInfo

import requests

from task_scheduler import TaskScheduler

from . import CLOUDFLARE_HEADERS, CLOUDFLARE_DNS_UPDATE_URL, DYN_DNS_FILE, dyn_dns_details

class DynDns:
    def __init__(self, scheduler: TaskScheduler) -> None:
        # Store the scheduler
        self.scheduler = scheduler

        # Schedule the task to check the external IP every minute and update the DNS if it has changed
        self.scheduler.schedule_task(datetime.now(timezone.utc), self.update_dns, timedelta(minutes=1))

    def get_external_ip(self) -> str:
        # Get the current external IP
        try:
            response = requests.get(dyn_dns_details.get_external_ip_url)
        except requests.exceptions.RequestException as e:
            logging.error(f'Failed to get the external IP address. Exception: {e}')
            return dyn_dns_details.current_external_ip

        # If the request was successful, return the new IP address otherwise return the current one
        if response.status_code == 200:
            logging.info(f'External IP address currently {response.json()["ip"]}')
            return response.json()['ip']
        else:
            logging.error(f'Failed to get the external IP address. Status code: {response.status_code}')
            return dyn_dns_details.current_external_ip

    def update_cloudflare_dns(self, new_external_ip: str) -> bool:
        # Get the time in the Europe/London timezone
        london_tz = ZoneInfo('Europe/London')
        london_time = datetime.now(london_tz)

        # Create the payload to update the DNS record
        payload = {
            "content": new_external_ip,
            "name": "schleising.net",
            "proxied": True,
            "type": "A",
            "comment": f"Updated to {new_external_ip} on {london_time.strftime('%Y-%m-%d %H:%M:%S')}",
        }

        # Log the new external IP
        logging.error(f'Updating DNS record to {new_external_ip}.')

        # Send the request to update the DNS record
        try:
            response = requests.request("PATCH", CLOUDFLARE_DNS_UPDATE_URL, json=payload, headers=CLOUDFLARE_HEADERS)
        except requests.exceptions.RequestException as e:
            logging.error(f'Failed to update the DNS record. Exception: {e}')
            return False

        if response.status_code == 200:
            logging.error(f'DNS record updated successfully to {new_external_ip}.')
            return True
        else:
            logging.error(f'Failed to update the DNS record. Status code: {response.status_code}')
            return False

    def update_dns(self) -> None:
        # Get the new external IP
        new_external_ip = self.get_external_ip()

        # If the current external IP is different to the one stored in the dyn dns details, update the dns
        if new_external_ip != dyn_dns_details.current_external_ip:
            # Log that the external IP has changed
            logging.error(f'External IP address has changed from {dyn_dns_details.current_external_ip} to {new_external_ip}.')
            if self.update_cloudflare_dns(new_external_ip):
                # If the DNS record was updated successfully, update the current external IP
                dyn_dns_details.current_external_ip = new_external_ip

                # Save the new external IP to the file
                with open(DYN_DNS_FILE, 'w', encoding='utf-8') as dyn_dns_file:
                    dyn_dns_file.write(dyn_dns_details.model_dump_json(indent=2))

def dyn_dns_loop(terminate_event: Event) -> None:
    # Create a task scheduler
    scheduler = TaskScheduler()

    # Create a DynDns object
    DynDns(scheduler)

    # Run the task scheduler
    scheduler.run(terminate_event)
