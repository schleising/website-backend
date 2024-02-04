from datetime import datetime, timedelta, timezone
import logging
from threading import Event
from zoneinfo import ZoneInfo

import requests

from notify_run import Notify

from task_scheduler import TaskScheduler

from . import CLOUDFLARE_HEADERS, CLOUDFLARE_DNS_UPDATE_URL, NOTIFY_RUN_ENDPOINT, dyn_dns_details, DNS_INFO_COLLECTION

class DynDns:
    def __init__(self, scheduler: TaskScheduler) -> None:
        # Store the scheduler
        self.scheduler = scheduler

        # Schedule the task to check the external IP every minute and update the DNS if it has changed
        self.scheduler.schedule_task(datetime.now(timezone.utc), self.update_dns, timedelta(minutes=1))

        # If the notify run endpoint is set, initialise the notify run object
        if NOTIFY_RUN_ENDPOINT:
            self.notify = Notify(endpoint=NOTIFY_RUN_ENDPOINT)
        else:
            self.notify = None

    def get_external_ip(self) -> str:
        # Get the current external IP
        try:
            response = requests.get(dyn_dns_details.get_external_ip_url)
        except requests.exceptions.RequestException as e:
            logging.error(f'Failed to get the external IP address. Exception: {e}')
            return dyn_dns_details.current_external_ip

        # If the request was successful, return the new IP address otherwise return the current one
        if response.status_code == 200:
            logging.debug(f'External IP address currently {response.json()["ip"]}')
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
        logging.info(f'Updating DNS record to {new_external_ip}.')

        # Send the request to update the DNS record
        try:
            response = requests.request("PATCH", CLOUDFLARE_DNS_UPDATE_URL, json=payload, headers=CLOUDFLARE_HEADERS)
        except requests.exceptions.RequestException as e:
            logging.error(f'Failed to update the DNS record. Exception: {e}')
            return False

        if response.status_code == 200:
            logging.info(f'DNS record updated successfully to {new_external_ip}.')
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
            logging.info(f'External IP address has changed from {dyn_dns_details.current_external_ip} to {new_external_ip}.')

            if self.update_cloudflare_dns(new_external_ip):
                # If the notify run endpoint is set, send a notification
                if self.notify:
                    self.notify.send(f'DNS Update Succeeded\nIP Address Changed from {dyn_dns_details.current_external_ip} to {new_external_ip}.')

                # If the DNS record was updated successfully, update the current external IP
                dyn_dns_details.current_external_ip = new_external_ip

                # Update the database with the new external IP
                if DNS_INFO_COLLECTION is not None:
                    DNS_INFO_COLLECTION.update_one({}, {'$set': {'current_external_ip': new_external_ip}})
            else:
                # Send a notification if the notify run endpoint is set
                if self.notify:
                    self.notify.send(f'DNS Update Failed\nTried to update to {new_external_ip} but it failed. Current IP Address is {dyn_dns_details.current_external_ip}.')

def dyn_dns_loop(terminate_event: Event) -> None:
    # Create a task scheduler
    scheduler = TaskScheduler()

    # Create a DynDns object
    DynDns(scheduler)

    # Run the task scheduler
    scheduler.run(terminate_event)
