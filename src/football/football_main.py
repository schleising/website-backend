from datetime import datetime, timezone
from threading import Event
import logging

from task_scheduler import TaskScheduler
from utils.network_utils import DailyApiRetryScheduler, FOOTBALL_API_MIN_INTERVAL

from . import Football
from .world_cup import WorldCup


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
    for index, task in enumerate(bootstrap_tasks):
        scheduler.schedule_task(now + FOOTBALL_API_MIN_INTERVAL * index, task)


def football_loop(terminate_event: Event, log_level: int) -> None:
    logging.basicConfig(format='Football: %(asctime)s - %(levelname)s - %(message)s', level=log_level)

    scheduler = TaskScheduler()
    daily_retry = DailyApiRetryScheduler(scheduler)

    football = Football(scheduler, daily_retry)
    world_cup = WorldCup(scheduler, daily_retry)

    schedule_football_bootstrap(scheduler, football, world_cup)

    scheduler.run(terminate_event)
