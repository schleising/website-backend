from threading import Event
import logging

from task_scheduler import TaskScheduler

from . import Football
from .world_cup import WorldCup

def football_loop(terminate_event: Event, log_level: int) -> None:
    # Initialise logging
    logging.basicConfig(format='Football: %(asctime)s - %(levelname)s - %(message)s', level=log_level)

    # Get a TaskScheduler object
    scheduler = TaskScheduler()

    # Create the football objects with the scheduler
    Football(scheduler)
    WorldCup(scheduler)

    # Run the scheduled tasks
    scheduler.run(terminate_event)
