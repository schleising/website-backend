from __future__ import annotations

from threading import Event
import logging

from task_scheduler import TaskScheduler

from .feeds import Feeds


def feeds_loop(terminate_event: Event, log_level: int) -> None:
    """Start the feed worker scheduler loop."""

    logging.basicConfig(
        format="Feeds: %(asctime)s - %(levelname)s - %(message)s",
        level=log_level,
    )

    scheduler = TaskScheduler()
    Feeds(scheduler)
    scheduler.run(terminate_event)
