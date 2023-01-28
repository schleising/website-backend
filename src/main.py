from concurrent.futures import ThreadPoolExecutor
from threading import Event
from time import sleep
from signal import signal, SIGTERM, SIGINT, Signals
from types import FrameType
from typing import Any
import logging

from football import Football

def terminate(signal: int, _: FrameType | None) -> Any:
    # Initialise sig_type to UNKNOWN
    sig_type = 'UNKNOWN'

    # Change the sig_type into a string
    match signal:
        case Signals.SIGINT:
            sig_type = 'SIGINT'
        case Signals.SIGTERM:
            sig_type = 'SIGTERM'

    # Log the reason for exiting
    logging.info(f'Exiting Threads due to {sig_type}')

    # Set the terminate event
    terminate_event.set()

    # Wait for the threads to finish
    for future in futures:
        while not future.done():
            sleep(0.0001)

    # Log that the threads have exited
    logging.info('Threads Exited')

if __name__ == '__main__':
    # Initialise an empty futures array
    futures = []

    # Event to terminate threads
    terminate_event = Event()

    # Initialise logging
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

    # Handle SIGTERM and SIGINT
    signal(SIGTERM, terminate)
    signal(SIGINT, terminate)

    football = Football()

    # Log that the backend is intialised
    logging.info('Backend Initialising')

    # Submit the futures
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(football.print_time, i, terminate_event) for i in range(5)]
