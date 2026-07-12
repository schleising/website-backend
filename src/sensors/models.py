from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator


# The instance type of the capability, e.g. "online" or "sensorTemperature"
class InstanceType(str, Enum):
    ONLINE = "online"
    TEMPERATURE = "sensorTemperature"
    HUMIDITY = "sensorHumidity"


# The state of the capability, e.g. the temperature value
# Offline devices can return an empty string for sensor readings.
class State(BaseModel):
    value: float | bool | None

    @field_validator("value", mode="before")
    @classmethod
    def empty_string_to_none(cls, value: Any) -> Any:
        if value == "":
            return None
        return value


# The container for the capability data
class Capability(BaseModel):
    type: str
    instance: InstanceType
    state: State | None = None


# The device information
class Device(BaseModel):
    sku: str
    device: str
    device_name: str = Field(alias="deviceName")
    type: str
    capabilities: list[Capability]


# The response from the Govee API for the list of devices
class GoveeDeviceResponse(BaseModel):
    code: int
    message: str
    data: list[Device]


# The payload for the status request
class GoveeStatusRequestPayload(BaseModel):
    sku: str
    device: str
    capabilities: list[Capability] | None = None


# The request to get the status of a device
class GoveeStatusRequest(BaseModel):
    request_id: str = Field(default="uuid", alias="requestId")
    payload: GoveeStatusRequestPayload


# The response from the Govee API for the status request
class GoveeStatusResponse(BaseModel):
    request_id: str = Field(alias="requestId")
    code: int
    msg: str
    payload: GoveeStatusRequestPayload


# Measurement data from the sensor stored in the database
class SensorData(BaseModel):
    device_name: str
    timestamp: datetime
    online: bool
    temperature: float
    humidity: float
