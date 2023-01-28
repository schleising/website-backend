import logging
from threading import Event
from time import sleep

class Football:
    def __init__(self) -> None:
        pass

    def print_time(self, id: int, terminate_event: Event) -> None:
        while not terminate_event.is_set():
            logging.info(f'Thread ID: {id}')
            sleep(1)

