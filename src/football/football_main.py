from threading import Event

from task_scheduler import TaskScheduler

from . import Football

def football_loop(terminate_event: Event) -> None:
    # Get a TaskScheduler object
    scheduler = TaskScheduler()

    # Create the football object with the scheduler
    Football(scheduler)

    # Run the scheduled tasks
    scheduler.run(terminate_event)
