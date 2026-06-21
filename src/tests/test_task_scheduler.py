import unittest
from datetime import datetime, timedelta, timezone

from task_scheduler.task_scheduler import TaskScheduler


def _task_callback() -> None:
    pass


def _other_task_callback() -> None:
    pass


class TestScheduleEarlierTask(unittest.TestCase):
    def setUp(self) -> None:
        self.scheduler = TaskScheduler()
        self.now = datetime.now(timezone.utc)
        self.callback = _task_callback

    def test_adds_when_no_existing_task(self) -> None:
        later = self.now + timedelta(hours=1)
        self.assertTrue(
            self.scheduler.schedule_earlier_task(later, self.callback)
        )
        self.assertEqual(len(self.scheduler.task_list), 1)
        self.assertIs(self.scheduler.task_list[0].function, self.callback)

    def test_ignores_later_schedule(self) -> None:
        earlier = self.now + timedelta(hours=1)
        later = self.now + timedelta(hours=2)
        self.scheduler.schedule_earlier_task(earlier, self.callback)
        self.assertFalse(self.scheduler.schedule_earlier_task(later, self.callback))
        self.assertEqual(self.scheduler.task_list[0].time, earlier)

    def test_ignores_equal_schedule(self) -> None:
        when = self.now + timedelta(hours=1)
        self.scheduler.schedule_earlier_task(when, self.callback)
        self.assertFalse(self.scheduler.schedule_earlier_task(when, self.callback))

    def test_replaces_with_earlier_schedule(self) -> None:
        later = self.now + timedelta(hours=2)
        earlier = self.now + timedelta(hours=1)
        self.scheduler.schedule_earlier_task(later, self.callback)
        self.assertTrue(self.scheduler.schedule_earlier_task(earlier, self.callback))
        self.assertEqual(self.scheduler.task_list[0].time, earlier)

    def test_replace_updates_interval(self) -> None:
        later = self.now + timedelta(days=1)
        earlier = self.now + timedelta(hours=1)
        daily = timedelta(days=1)
        self.scheduler.schedule_earlier_task(
            later, self.callback, interval=daily
        )
        self.scheduler.schedule_earlier_task(earlier, self.callback, interval=None)
        self.assertIsNone(self.scheduler.task_list[0].interval)

    def test_separate_callbacks_do_not_collide(self) -> None:
        when = self.now + timedelta(hours=1)
        self.scheduler.schedule_earlier_task(when, _task_callback)
        self.scheduler.schedule_earlier_task(when, _other_task_callback)
        self.assertEqual(len(self.scheduler.task_list), 2)


class TestPeriodicRequeue(unittest.TestCase):
    def test_periodic_task_requeues_after_run(self) -> None:
        scheduler = TaskScheduler()
        calls: list[int] = []

        def periodic() -> None:
            calls.append(1)

        start = datetime.now(timezone.utc) - timedelta(seconds=1)
        scheduler.schedule_task(start, periodic, timedelta(milliseconds=50))

        import time

        for _ in range(3):
            for task in scheduler.get_runnable_tasks():
                task.function()
            time.sleep(0.06)

        self.assertGreaterEqual(len(calls), 2)
        self.assertEqual(len(scheduler.task_list), 1)
        self.assertIs(scheduler.task_list[0].function, periodic)
        self.assertIsNotNone(scheduler.task_list[0].interval)


if __name__ == "__main__":
    unittest.main()
