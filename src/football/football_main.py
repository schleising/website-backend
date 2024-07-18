from threading import Event
import logging

from task_scheduler import TaskScheduler

from . import Football

def football_loop(terminate_event: Event, log_level: int) -> None:
    # Initialise logging
    logging.basicConfig(format='Football: %(asctime)s - %(levelname)s - %(message)s', level=log_level)

    # Get a TaskScheduler object
    scheduler = TaskScheduler()

    # Create the football object with the scheduler
    Football(scheduler)

    # Run the scheduled tasks
    scheduler.run(terminate_event)
