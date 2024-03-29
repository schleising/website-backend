from threading import Event
from time import sleep
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable
import logging

@dataclass
class Task:
    time: datetime
    function: Callable
    interval: timedelta | None = None

class TaskScheduler:
    def __init__(self) -> None:
        # Initialise an empty list of tasks
        self.task_list: list[Task] = []

    def schedule_task(self, time: datetime, function: Callable, interval: timedelta | None = None) -> bool:
        # Ensure the task run time is stored in UTC
        utc_time = time.astimezone(timezone.utc)

        # If the task is in the past, set it to now so it runs immediately and any scheduling uses this as its basis
        if utc_time < datetime.now(timezone.utc):
            utc_time = datetime.now(timezone.utc)

        # Add the task
        self.task_list.append(Task(utc_time, function, interval))
        logging.debug(f'Task added')
        return True

    def get_runnable_tasks(self) -> list[Task]:
        # Initialise an empty list of runnable tasks
        runnable_tasks:list[Task] = []

        # Check the list of tasks to see which are runnable, use a copy as we are going to remove old tasks
        for task in list(self.task_list):

            # If the task time is in the past it is runnable
            if task.time < datetime.now(timezone.utc):

                # Check whether the task is periodic
                if task.interval is not None:
                    # If so schedule the next iteration of the task
                    self.schedule_task(task.time + task.interval, task.function, task.interval)

                # Add the task to the list of runnable tasks
                runnable_tasks.append(task)

                # Remove the old task from the list
                self.task_list.remove(task)

        return runnable_tasks

    def run(self, terminate_event: Event) -> None:
        # Infinite loop until the terminate event gets set
        while not terminate_event.is_set():
            # Get the list of runnable tasks
            task_list = self.get_runnable_tasks()

            # Run the tasks
            for task in task_list:
                task.function()

            # Sleep for a time
            sleep(0.01)
