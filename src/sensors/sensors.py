from datetime import datetime, timedelta, timezone
import logging
from threading import Event

from pydantic import ValidationError
import requests
from requests.exceptions import RequestException

from pymongo.errors import ServerSelectionTimeoutError, NetworkTimeout, AutoReconnect

from task_scheduler import TaskScheduler

from utils import farenheit_to_celsius

from .models import (
    Device,
    GoveeStatusRequestPayload,
    GoveeStatusRequest,
    GoveeStatusResponse,
    InstanceType,
    SensorData,
)

from . import (
    SENSORS_COLLECTION,
    SENSOR_DATA_COLLECTION,
    GOVEE_API_KEY,
    DEVICE_STATE_URL,
)


class Sensors:
    def __init__(self, scheduler: TaskScheduler):
        # Store the scheduler
        self.scheduler = scheduler

        # Schedule the task to check the external IP every minute and update the DNS if it has changed
        self.scheduler.schedule_task(
            datetime.now(timezone.utc), self._get_sensors_data, timedelta(minutes=5)
        )

    def _get_sensors_data(self):
        # Get the list of sensors from the database
        if SENSORS_COLLECTION is not None:
            # Get the sensors from the database ignoring the _id field
            try:
                sensors_db = SENSORS_COLLECTION.find({})

            except (ServerSelectionTimeoutError, NetworkTimeout, AutoReconnect):
                logging.error("Failed to connect to the database and get the sensors.")
                return

            try:
                # Convert the sensors to a list of Device objects
                sensors = [Device.model_validate(sensor) for sensor in sensors_db]
            except ValidationError as e:
                logging.error(
                    f"Failed to validate the sensors from the database. Exception: {e}"
                )
                logging.error(e.json(indent=2))
                return
        else:
            logging.error("Failed to get the sensors collection from the database.")
            return

        # If the sensors collection is empty, log an error and return
        if not sensors:
            logging.error("No sensors found in the database.")
            return

        # Get the sensor data
        for sensor in sensors:
            # Create a request to get the status of the device
            payload = GoveeStatusRequestPayload(sku=sensor.sku, device=sensor.device)
            request = GoveeStatusRequest(payload=payload)
            data = request.model_dump(by_alias=True)

            # Get the status of the device
            try:
                response = requests.post(
                    DEVICE_STATE_URL,
                    headers={
                        "Content-Type": "application/json",
                        "Govee-API-Key": GOVEE_API_KEY,
                    },
                    json=data,
                )
            except RequestException as e:
                logging.error(
                    f"Failed to get the status of the device: {sensor.device_name}, error: {e}"
                )
                continue

            # Check if the request was successful
            if response.status_code != requests.codes.ok:
                logging.error(
                    f"Failed to get the status of the device: {sensor.device_name}"
                )
                continue

            try:
                # Parse the response into a GoveeStatusResponse object
                govee_status_response = GoveeStatusResponse.model_validate_json(
                    response.text
                )
            except ValidationError as e:
                logging.error(
                    f"{sensor.device_name} Failed to parse the response into a GoveeStatusResponse object: {response.text}"
                )
                logging.debug(e.json(indent=2))
                continue

            # Insert the sensor data into the database
            self._insert_sensor_data(sensor.device_name, govee_status_response)

    def _insert_sensor_data(
        self, device_name: str, govee_status_response: GoveeStatusResponse
    ):
        # Check if the capabilities are None
        if govee_status_response.payload.capabilities is None:
            return

        # Create a SensorData object
        sensor_data = SensorData(
            device_name=device_name,
            timestamp=datetime.now(timezone.utc),
            online=False,
            temperature=0.0,
            humidity=0.0,
        )

        # Iterate over the capabilities
        for capability in govee_status_response.payload.capabilities:
            if capability.state is None:
                continue

            match capability.instance, capability.state.value:
                case InstanceType.ONLINE, bool(online):
                    # Set the online status
                    sensor_data.online = online

                    # Log the online status
                    logging.debug(f"{device_name} online: {online}")
                case InstanceType.TEMPERATURE, float(temperature):
                    # Convert the temperature to celsius
                    celsius = farenheit_to_celsius(temperature)

                    # Add the temperature to the sensor data
                    sensor_data.temperature = celsius

                    # Log the temperature
                    logging.debug(f"{device_name} temperature: {celsius:.1f}°C")
                case InstanceType.HUMIDITY, float(humidity):
                    # Add the humidity to the sensor data
                    sensor_data.humidity = humidity

                    # Log the humidity
                    logging.debug(f"{device_name} humidity: {humidity:.1f}%")
                case _:
                    # Log the unknown capability
                    logging.error(
                        f"Unknown capability: {capability.type}, Value: {capability.state.value}"
                    )

        # Insert the sensor data into the database
        if SENSOR_DATA_COLLECTION is not None:
            try:
                SENSOR_DATA_COLLECTION.insert_one(sensor_data.model_dump(by_alias=True))
            except (ServerSelectionTimeoutError, NetworkTimeout, AutoReconnect):
                logging.error(
                    "Failed to connect to the database and insert the sensor data."
                )
                return


def sensors_loop(terminate_event: Event, log_level: int):
    # Initialise logging
    logging.basicConfig(
        format="Sensors: %(asctime)s - %(levelname)s - %(message)s", level=log_level
    )

    # Create a task scheduler
    scheduler = TaskScheduler()

    # Create a DynDns object
    Sensors(scheduler)

    # Run the task scheduler
    scheduler.run(terminate_event)
