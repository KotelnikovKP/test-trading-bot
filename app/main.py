import asyncio
import time
from datetime import datetime, timezone, timedelta

import aiohttp
from sqlalchemy import select

from app.aiohttp_handlers import request_async, execute_gather
from app.bybit import Bybit
from app.config import LIMIT_PER_HOST, LIMIT, TTL_DNS_CACHE
from app.handlers import IterationStack
from app.models import Symbol, get_async_session
from app.scheduler import AsyncScheduler


async def schedule_event_handler():
    now = datetime.utcnow() - timedelta(seconds=30)
    time_kline = datetime(now.year, now.month, now.day, now.hour, now.minute, 0, 0, tzinfo=timezone.utc)
    iteration = iteration_stack.add_iteration(time_kline)

    async with async_db_session() as session:
        symbols = await session.execute(
            select(Symbol).
            where(Symbol.is_active)
        )
        if symbols:
            requests = [
                bybit.get_kline(category='linear', symbol=s.symbol, interval=1, limit='2')
                for s in symbols
            ]
        else:
            requests = []

    conn = aiohttp.TCPConnector(limit_per_host=LIMIT_PER_HOST, limit=LIMIT, ttl_dns_cache=TTL_DNS_CACHE)
    aiohttp_session = aiohttp.ClientSession(connector=conn)
    await execute_gather(
        *(request_async(aiohttp_session, *request, iteration_stack.request_result_handler) for request in requests)
    )
    await conn.close()

    iteration_stack.calculate_indicators(iteration)

    iteration_stack.make_decision(iteration)

    await iteration.save_to_db(async_db_session)


async def launch_scheduler_tasks():
    await iteration_stack.get_kline_history(async_db_session)
    main_scheduler = AsyncScheduler()
    await main_scheduler.create_and_run_async_job(
        'schedule_event_handler',
        '*/1 * * * *',
        schedule_event_handler,
        delay=1.00
    )
    await main_scheduler.create_and_run_job(
        'garbage_collector',
        '*/1 * * * *',
        iteration_stack.garbage_collector,
        delay=45.00
    )


if __name__ == '__main__':

    async_db_session = get_async_session()
    bybit = Bybit()

    iteration_stack = IterationStack()

    loop = asyncio.new_event_loop()
    loop.create_task(launch_scheduler_tasks())

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()
