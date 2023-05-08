import asyncio
import time
from datetime import datetime, timezone, timedelta

import aiohttp
from sqlalchemy import select

from app.aiohttp_handlers import request_async, execute_gather
from app.bybit import Bybit
from app.config import LIMIT_PER_HOST, LIMIT, TTL_DNS_CACHE, DEBUG
from app.handlers import IterationStack
from app.models import Symbol, get_async_session
from app.scheduler import AsyncScheduler


async def schedule_event_handler():
    """
    The main handler
    It gets actually kline history data from Bybit exchange, saves them in iteration stack, calculates indicators,
    makes decision, annotates expected results and saves data to database
    """

    if DEBUG:
        print(f"{datetime.utcnow()} Start iteration")

    # Create new iteration in iteration stack with time one minute ago
    now = datetime.utcnow() - timedelta(seconds=30)
    time_kline = datetime(now.year, now.month, now.day, now.hour, now.minute, 0, 0, tzinfo=timezone.utc)
    iteration = iteration_stack.add_iteration(time_kline)

    # Get list of symbols form databasec for tracking and calculation
    async with async_db_session() as session:
        symbols = await session.execute(
            select(Symbol).
            where(Symbol.is_active)
        )

        # Prepare list of requests to Bybit exchange by bybit.py module (API connector for Bybit HTTP API v.5)
        # Use method Get Kline (https://bybit-exchange.github.io/docs/v5/market/kline)
        # for get last full minute kline data for each symbol
        if symbols:
            requests = [
                bybit.get_kline(category='linear', symbol=s.symbol, interval=1, limit='2')
                for s in symbols.scalars()
            ]
        else:
            requests = []

    if DEBUG:
        print(f"  {datetime.utcnow()} Symbols have been loaded from database, requests have been prepared")

    # Make requests to exchange in asynchronous mode for all symbols together by aiohttp_handlers.py module
    # Declare iteration_stack.request_result_handler as handler received results,
    # it will be calls for result for each symbols

    if DEBUG:
        s = time.perf_counter()

    conn = aiohttp.TCPConnector(limit_per_host=LIMIT_PER_HOST, limit=LIMIT, ttl_dns_cache=TTL_DNS_CACHE)
    aiohttp_session = aiohttp.ClientSession(connector=conn)
    await execute_gather(
        *(request_async(aiohttp_session, *request, iteration_stack.request_result_handler) for request in requests)
    )
    await conn.close()

    if DEBUG:
        elapsed = time.perf_counter() - s
        print(f"  {datetime.utcnow()} Requests have been processed, processing time = {elapsed}")

    # After receiving data from exchange calculate indicators for each kline in current iteration
    iteration_stack.calculate_indicators(iteration)

    # After calculate indicators make decision
    # In this case, about reaching the price change threshold without BTC impact
    # If successful, annotate it
    iteration_stack.make_decision(iteration)

    if DEBUG:
        print(f"  {datetime.utcnow()} Indicators have been calculated, decisions have been made")

    # Save all received and calculated data to database for future use
    if DEBUG:
        print(f"  {datetime.utcnow()} Data is saving to database:")

    await iteration.save_to_db(async_db_session)

    if DEBUG:
        print(f"  {datetime.utcnow()} Data have been saved to database. Iteration has been finished.")
        print(' ')


async def launch_scheduler_tasks():

    if DEBUG:
        print(f"{datetime.utcnow()} Start program")

    # Load existing kline history in memory (in iteration stack) form database
    await iteration_stack.get_kline_history(async_db_session)
    if DEBUG:
        print(f"{datetime.utcnow()} Existing kline history have been uploaded in iteration stack")

    # Create asynchronous scheduler
    main_scheduler = AsyncScheduler()

    # Put in scheduler main handler (run every minute with delay 1 second)
    await main_scheduler.create_and_run_async_job(
        'schedule_event_handler',
        '*/1 * * * *',
        schedule_event_handler,
        delay=1.00
    )

    # Put in scheduler garbage collector (run every minute with delay 45 second)
    # It frees memory from old kline history
    await main_scheduler.create_and_run_job(
        'garbage_collector',
        '*/1 * * * *',
        iteration_stack.garbage_collector,
        delay=45.00
    )


if __name__ == '__main__':

    async_db_session = get_async_session()
    bybit = Bybit()

    # Create an iteration stack - an array (dictionary) for temporary storage and all calculation of kline history
    iteration_stack = IterationStack()

    # Make event loop and launch the first procedure make scheduler tasks
    loop = asyncio.new_event_loop()
    loop.create_task(launch_scheduler_tasks())

    # Run never ended loop
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()
