import logging

import requests

from pymongo.errors import DuplicateKeyError

from database.database import BackendDatabase

from .models import GoveeDeviceResponse

BASE_URL = "https://openapi.api.govee.com/router/api/v1/"
DEVICE_LIST_URL = BASE_URL + "user/devices"
DEVICE_STATE_URL = BASE_URL + "device/state"

# Read the Govee API key from the file
with open("src/secrets/govee_api_key.txt", "r") as file:
    try:
        GOVEE_API_KEY = file.read().strip()
    except IOError as e:
        logging.error(f"Failed to read the Govee API key. Exception: {e}")
        GOVEE_API_KEY = None

# Get a backend database instance
DATABASE = BackendDatabase()

# Set the database in use
DATABASE.set_database("web_database")

# Set the collection in use
SENSORS_COLLECTION = DATABASE.get_collection("sensors_collection")

# Create an index on the sensors collection with the device field as the unique key
if SENSORS_COLLECTION is not None:
    SENSORS_COLLECTION.create_index("device", unique=True)

# Get a collection of sensor readings
SENSOR_DATA_COLLECTION = DATABASE.get_collection("sensor_data")

if SENSORS_COLLECTION is not None:
    # Get the sensors from the API
    if GOVEE_API_KEY is not None:
        response = requests.get(
            DEVICE_LIST_URL,
            headers={
                "Govee-API-Key": GOVEE_API_KEY,
                "Content-Type": "application/json",
            },
        )

        if response.status_code == requests.codes.ok:
            try:
                # Parse the response
                devices = GoveeDeviceResponse.model_validate_json(response.text).data
            except Exception as e:
                logging.error(
                    f"Failed to parse the Govee device response. Exception: {e}"
                )
                devices = None

            if devices is not None:
                # Insert the devices into the database
                for device in devices:
                    try:
                        SENSORS_COLLECTION.insert_one(device.model_dump(by_alias=True))
                    except DuplicateKeyError as e:
                        # If the device already exists, ignore the error
                        pass

from .sensors import sensors_loop

__all__ = ["sensors_loop"]
