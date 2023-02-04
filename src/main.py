from concurrent.futures import ThreadPoolExecutor, wait, FIRST_EXCEPTION, Future
from threading import Event
from time import sleep
from signal import signal, SIGTERM, SIGINT, Signals
from types import FrameType
import logging

from football import football_loop

def terminate(signal: int, _: FrameType | None) -> None:
    # Change the sig_type into a string
    match signal:
        case Signals.SIGINT:
            sig_type = 'SIGINT'
        case Signals.SIGTERM:
            sig_type = 'SIGTERM'
        case _:
            sig_type = 'UNKNOWN'

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
    futures: list[Future] = []

    # Event to terminate threads
    terminate_event = Event()

    # Initialise logging
    logging.basicConfig(format='Backend: %(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

    # Handle SIGTERM and SIGINT
    signal(SIGTERM, terminate)
    signal(SIGINT, terminate)

    # Log that the backend is intialised
    logging.info('Backend Initialising')

    # Submit the futures
    with ThreadPoolExecutor() as executor:
        futures.append(executor.submit(football_loop, terminate_event))

        # If any of the threads raises an exception then exit to output it
        wait(futures, return_when=FIRST_EXCEPTION)

        for future in futures:
            # Print the results, including any exceptions
            print(future.result())
