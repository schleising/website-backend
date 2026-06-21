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

    @staticmethod
    def _utc_task_time(time: datetime) -> datetime:
        utc_time = time.astimezone(timezone.utc)
        if utc_time < datetime.now(timezone.utc):
            utc_time = datetime.now(timezone.utc)
        return utc_time

    def schedule_task(self, time: datetime, function: Callable, interval: timedelta | None = None) -> bool:
        utc_time = self._utc_task_time(time)
        self.task_list.append(Task(utc_time, function, interval))
        logging.debug(f'Task added')
        return True

    def schedule_earlier_task(
        self, time: datetime, function: Callable, interval: timedelta | None = None
    ) -> bool:
        """Schedule a task, keeping at most one pending task per callback.

        If no task exists for ``function``, add one. If a task exists and the new
        time is earlier, replace it. If the new time is equal or later, ignore.
        """
        utc_time = self._utc_task_time(time)

        for index, task in enumerate(self.task_list):
            if task.function is not function:
                continue

            if utc_time >= task.time:
                logging.debug("Task ignored (existing task is earlier or equal)")
                return False

            self.task_list[index] = Task(utc_time, function, interval)
            logging.debug("Task replaced with earlier time")
            return True

        self.task_list.append(Task(utc_time, function, interval))
        logging.debug("Task added")
        return True

    def get_runnable_tasks(self) -> list[Task]:
        # Initialise an empty list of runnable tasks
        runnable_tasks:list[Task] = []

        # Check the list of tasks to see which are runnable, use a copy as we are going to remove old tasks
        for task in list(self.task_list):

            # If the task time is in the past it is runnable
            if task.time < datetime.now(timezone.utc):

                runnable_tasks.append(task)
                self.task_list.remove(task)

                # Re-queue periodic tasks after removing the runnable one — otherwise
                # schedule_earlier_task sees the same callback still pending and ignores.
                if task.interval is not None:
                    self.schedule_earlier_task(
                        task.time + task.interval, task.function, task.interval
                    )

        return runnable_tasks

    def run(self, terminate_event: Event) -> None:
        # Infinite loop until the terminate event gets set
        while not terminate_event.is_set():
            # Get the list of runnable tasks
            task_list = self.get_runnable_tasks()

            # Run the tasks
            for task in task_list:
                task.function()

            # Sleep for a time
            sleep(0.01)
