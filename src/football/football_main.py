from collections.abc import Callable
from datetime import datetime, timezone
from threading import Event
import logging

from task_scheduler import TaskScheduler
from utils.network_utils import DailyApiRetryScheduler, FOOTBALL_API_MIN_INTERVAL

from . import Football
from .world_cup import WorldCup

logger = logging.getLogger(__name__)


def _wrap_bootstrap_task(
    task: Callable[[], None],
    *,
    remaining: list[int],
) -> Callable[[], None]:
    def wrapped() -> None:
        try:
            task()
        finally:
            remaining[0] -= 1
            if remaining[0] == 0:
                logger.info("Football API startup requests completed")

    wrapped.__name__ = task.__name__
    return wrapped


def schedule_football_bootstrap(
    scheduler: TaskScheduler,
    football: Football,
    world_cup: WorldCup,
) -> None:
    now = datetime.now(timezone.utc)
    bootstrap_tasks = [
        football.get_table,
        football.get_season_matches,
        football.get_todays_matches,
        world_cup.sync_matches,
        world_cup.sync_standings,
        world_cup.get_todays_matches,
    ]
    remaining = [len(bootstrap_tasks)]
    for index, task in enumerate(bootstrap_tasks):
        scheduler.schedule_task(
            now + FOOTBALL_API_MIN_INTERVAL * index,
            _wrap_bootstrap_task(task, remaining=remaining),
        )


def football_loop(terminate_event: Event, log_level: int) -> None:
    logging.basicConfig(format='Football: %(asctime)s - %(levelname)s - %(message)s', level=log_level)

    scheduler = TaskScheduler()
    daily_retry = DailyApiRetryScheduler(scheduler)

    football = Football(scheduler, daily_retry)
    world_cup = WorldCup(scheduler, daily_retry)

    schedule_football_bootstrap(scheduler, football, world_cup)

    scheduler.run(terminate_event)
