from datetime import datetime, timedelta, timezone
import logging
from threading import Event
from zoneinfo import ZoneInfo

import requests

from notify_run import Notify

from task_scheduler import TaskScheduler

from . import (
    SYNOLOGY_API_BASE_URL,
    SYNOLOGY_AUTH_API,
    SYNOLOGY_AUTH_URL,
    SYNOLOGY_AUTH_VERSION,
    SYNOLOGY_AUTH_LOGIN_METHOD,
    SYNOLOGY_AUTH_LOGOUT_METHOD,
    SYNOLOGY_GET_EXTERNAL_IP_API,
    SYNOLOGY_GET_EXTERNAL_IP_URL,
    SYNOLOGY_GET_EXTERNAL_IP_VERSION,
    SYNOLOGY_GET_EXTERNAL_IP_METHOD,
    CLOUDFLARE_HEADERS,
    CLOUDFLARE_DNS_UPDATE_URL,
    NOTIFY_RUN_ENDPOINT,
    dyn_dns_details,
    DNS_INFO_COLLECTION,
)


class DynDns:
    def __init__(self, scheduler: TaskScheduler) -> None:
        # Store the scheduler
        self.scheduler = scheduler

        # Schedule the task to check the external IP every minute and update the DNS if it has changed
        self.scheduler.schedule_task(
            datetime.now(timezone.utc), self.update_dns, timedelta(seconds=10)
        )

        # If the notify run endpoint is set, initialise the notify run object
        if NOTIFY_RUN_ENDPOINT:
            self.notify = Notify(endpoint=NOTIFY_RUN_ENDPOINT)
        else:
            self.notify = None

    def get_external_ip(self) -> str:
        # Check whether the dyn dns details are set
        if dyn_dns_details is None:
            logging.error("Dyn DNS details are not set.")
            return ""

        # Log into the Synology API
        try:
            response = requests.get(
                url=f"{SYNOLOGY_API_BASE_URL}{SYNOLOGY_AUTH_URL}",
                params={
                    "api": SYNOLOGY_AUTH_API,
                    "version": SYNOLOGY_AUTH_VERSION,
                    "method": SYNOLOGY_AUTH_LOGIN_METHOD,
                    "account": dyn_dns_details.synology_username,
                    "passwd": dyn_dns_details.synology_password,
                },
            )
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to log into the Synology API (1). Exception: {e}")
            return dyn_dns_details.current_external_ip

        # If the request was unsuccessful, log the error and return the current external IP
        if response.status_code != requests.codes.ok:
            logging.error(
                f"Failed to log into the Synology API (2). Status code: {response.status_code}"
            )
            return dyn_dns_details.current_external_ip

        # Convert the response to json
        response_json = response.json()

        # Check whether the response was successful
        if response_json["success"] is False:
            logging.error(
                f"Failed to log into the Synology API (3). Error: {response_json['error']}"
            )
            return dyn_dns_details.current_external_ip

        # Put the session id in the cookies
        cookies = {"id": response_json["data"]["sid"]}

        # Get the external IP address
        try:
            response = requests.get(
                url=f"{SYNOLOGY_API_BASE_URL}{SYNOLOGY_GET_EXTERNAL_IP_URL}",
                params={
                    "api": SYNOLOGY_GET_EXTERNAL_IP_API,
                    "version": SYNOLOGY_GET_EXTERNAL_IP_VERSION,
                    "method": SYNOLOGY_GET_EXTERNAL_IP_METHOD,
                },
                cookies=cookies,
            )
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to get the external IP address (4). Exception: {e}")
            return dyn_dns_details.current_external_ip

        # If the request was unsuccessful, log the error and return the current external IP
        if response.status_code != requests.codes.ok:
            logging.error(
                f"Failed to get the external IP address (5). Status code: {response.status_code}"
            )
            return dyn_dns_details.current_external_ip

        # Convert the response to json
        response_json = response.json()

        # Check whether the response was successful
        if response_json["success"] is False:
            logging.error(
                f"Failed to get the external IP address (6). Error: {response_json['error']}"
            )
            return dyn_dns_details.current_external_ip

        # Get the external IP address
        external_ip_address = response_json["data"][0]["ip"]
        logging.debug(f"External IP address currently {external_ip_address}")

        # Log out of the Synology API
        try:
            response = requests.get(
                url=f"{SYNOLOGY_API_BASE_URL}{SYNOLOGY_AUTH_URL}",
                params={
                    "api": SYNOLOGY_AUTH_API,
                    "version": SYNOLOGY_AUTH_VERSION,
                    "method": SYNOLOGY_AUTH_LOGOUT_METHOD,
                },
                cookies=cookies,
            )
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to log out of the Synology API (7). Exception: {e}")

        # Convert the response to json
        response_json = response.json()

        # Check whether the response was successful
        if response_json["success"] is False:
            logging.error(
                f"Failed to log out of the Synology API (8). Error: {response_json['error']}"
            )

        return external_ip_address

    def update_cloudflare_dns(self, new_external_ip: str) -> bool:
        # Get the time in the Europe/London timezone
        london_tz = ZoneInfo("Europe/London")
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
        logging.info(f"Updating DNS record to {new_external_ip}.")

        # Send the request to update the DNS record
        try:
            response = requests.request(
                "PATCH",
                CLOUDFLARE_DNS_UPDATE_URL,
                json=payload,
                headers=CLOUDFLARE_HEADERS,
            )
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to update the DNS record. Exception: {e}")
            return False

        if response.status_code == 200:
            logging.info(f"DNS record updated successfully to {new_external_ip}.")
            return True
        else:
            logging.error(
                f"Failed to update the DNS record. Status code: {response.status_code}"
            )
            return False

    def update_dns(self) -> None:
        # Check whether the dyn dns details are set
        if dyn_dns_details is None:
            logging.error("Dyn DNS details are not set.")
            return

        # Get the new external IP
        new_external_ip = self.get_external_ip()

        # If the current external IP is different to the one stored in the dyn dns details, update the dns
        if new_external_ip != dyn_dns_details.current_external_ip:
            # Log that the external IP has changed
            logging.info(
                f"External IP address has changed from {dyn_dns_details.current_external_ip} to {new_external_ip}."
            )

            if self.update_cloudflare_dns(new_external_ip):
                # If the notify run endpoint is set, send a notification
                if self.notify:
                    self.notify.send(
                        f"DNS Update Succeeded\nIP Address Changed from {dyn_dns_details.current_external_ip} to {new_external_ip}."
                    )

                # If the DNS record was updated successfully, update the current external IP
                dyn_dns_details.current_external_ip = new_external_ip

                # Update the database with the new external IP
                if DNS_INFO_COLLECTION is not None:
                    DNS_INFO_COLLECTION.update_one(
                        {}, {"$set": {"current_external_ip": new_external_ip}}
                    )
            else:
                # Send a notification if the notify run endpoint is set
                if self.notify:
                    self.notify.send(
                        f"DNS Update Failed\nTried to update to {new_external_ip} but it failed. Current IP Address is {dyn_dns_details.current_external_ip}."
                    )


def dyn_dns_loop(terminate_event: Event, log_level: int) -> None:
    # Initialise logging
    logging.basicConfig(
        format="Dyn DNS: %(asctime)s - %(levelname)s - %(message)s", level=log_level
    )

    # Create a task scheduler
    scheduler = TaskScheduler()

    # Create a DynDns object
    DynDns(scheduler)

    # Run the task scheduler
    scheduler.run(terminate_event)
