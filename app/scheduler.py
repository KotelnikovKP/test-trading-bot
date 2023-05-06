"""
Async scheduler
Idea and the most part of code by Igor Kovalenko https://bitbucket.org/igor_kovalenko/async_scheduler/src/master/
Special thanks to Igor Kovalenko
There are minor edits from me
"""

import asyncio
import typing

from crontab import CronTab

from app.config import ADDED_DELAY


class Job:
    """
    Задание
    """

    _cb = None  # Функция, которая будет выполнена по расписанию
    _async_cb = None  # Короутина которая будет выполнена по расписанию
    _executor = None  # Диспетчер (executor) для выполнения блокирующего кода
    _schedule_entry = None  # Расписание
    _last_scheduled = None  # Дата-время последнего запуска задачи
    _delay = None  # Задержка выполнения в секундах
    _is_active = True
    _is_stopped = False
    _once = False
    _loop = None
    _job_id = 'unknown'

    on_stopped = None

    def __init__(self, schedule_entry: str, loop=None, once=False, cb: typing.Callable = None,
                 async_cb: typing.Callable = None, delay=0.00, executor=None, job_id: str = 'unknown'):

        self._schedule_entry = CronTab(schedule_entry)
        self._cb = cb
        self._async_cb = async_cb
        self._executor = executor
        self._loop = loop or asyncio.get_running_loop()
        self._once = once
        self._delay = delay
        self._job_id = job_id

    def _on_stopped(self):

        # Если событие on_stopped еще не происходило
        if not self._is_stopped:
            # Помечаем экземпляр как остановившийся и что событие on_stopped уже произошло
            self._is_stopped = True

            # Если определен обработчик события, то вызываем этот обработчик
            if self.on_stopped:
                # Если обработчик - короутина, то создаем задачу
                if isinstance(self.on_stopped, typing.Coroutine):
                    self._loop.create_task(self.on_stopped)

                # Если обработчик - функция, то вызываем ее
                if isinstance(self.on_stopped, typing.Callable):
                    self.on_stopped()

    async def run(self, is_first_run=True):

        if not self._is_active:
            self._on_stopped()
            return

        if not is_first_run:
            if self._async_cb:
                asyncio.create_task(self._async_cb())

            if self._cb:
                if self._executor:
                    self._loop.run_in_executor(self._executor, self._cb)
                else:
                    self._cb()

            if self._once:
                self._is_active = False
                return

        delay = self._schedule_entry.next(default_utc=True)
        if delay:
            await asyncio.sleep(delay + self._delay + ADDED_DELAY)
            asyncio.create_task(self.run(is_first_run=False))

    def stop(self):
        self._is_active = False

    @property
    def is_stopped(self):
        return self._is_stopped


class AsyncScheduler:
    __state = {}  # Общее состояние экземпляров класса
    _jobs = {}
    _stopped_jobs = []
    _loop = None

    def __init__(self, loop=None):
        self.__dict__ = self.__state
        self._loop = loop or asyncio.get_running_loop()

    def _on_job_stop_handler(self):
        """
        Обработчик события on_stopped.
        Производит удаление уже остановившихся заданий
        """
        new_stopped_jobs_list = []
        for idx in range(len(self._stopped_jobs)):
            if not self._stopped_jobs[idx].is_stopped:
                new_stopped_jobs_list.append(self._stopped_jobs[idx])
        self._stopped_jobs = new_stopped_jobs_list

    def delete_job(self, job_id):
        job = self._jobs.pop(job_id)

        # Перемещаем задание в отдельный список,
        # что бы дать ему возможность аккуратно завершиться...
        self._stopped_jobs.append(job)

        # ... и затем останавливаем его
        job.stop()

    async def create_and_run_job(self, job_id: str, schedule: str, cb: typing.Callable, delay=0.00,
                                 once: bool = False, executor=None):
        """
        Создать и запустить задание
        """

        # Если задание уже существует....
        if job_id in self._jobs:
            # ...то сперва удаляем его
            self.delete_job(job_id)

        # Создаем новое задание
        self._jobs[job_id] = Job(schedule_entry=schedule, cb=cb, once=once, delay=delay, executor=executor,
                                 job_id=job_id)

        self._jobs[job_id].on_stopped = self._on_job_stop_handler

        # Запускаем новое задание
        self._loop.create_task(self._jobs[job_id].run())

    async def create_and_run_async_job(self, job_id: str, schedule: str, cb: typing.Callable, delay=0.00,
                                       once: bool = False):
        """
        Создать и запустить асинхронное задание
        """

        # Если задание уже существует....
        if job_id in self._jobs:
            # ...то сперва удаляем его
            self.delete_job(job_id)

        # Создаем новое задание
        self._jobs[job_id] = Job(schedule_entry=schedule, async_cb=cb, once=once, delay=delay, job_id=job_id)

        self._jobs[job_id].on_stopped = self._on_job_stop_handler

        # Запускаем новое задание
        self._loop.create_task(self._jobs[job_id].run())
