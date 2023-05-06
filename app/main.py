import asyncio

import aiohttp
from sqlalchemy import select

from app.aiohttp_handlers import request_async, execute_gather
from app.bybit import Bybit
from app.config import LIMIT_PER_HOST, LIMIT, TTL_DNS_CACHE
from app.models import Symbol, get_async_session
from app.scheduler import AsyncScheduler


async def schedule_event_handler():
    conn = aiohttp.TCPConnector(limit_per_host=LIMIT_PER_HOST, limit=LIMIT, ttl_dns_cache=TTL_DNS_CACHE)
    aiohttp_session = aiohttp.ClientSession(connector=conn)

    results = {}

    async with async_db_session() as session:
        symbols = await session.execute(
            select(Symbol).
            where(Symbol.is_active == True)
        )
        if symbols:
            requests = [
                bybit.get_public_trade_history(category='linear', symbol=s.symbol, limit='1')
                for s in symbols.scalars()
            ]
        else:
            requests = []

    def handler1(status=None, result=None):
        results[result['result']['list'][0]['symbol']] = result['result']['list'][0]['price']

    await execute_gather(*(request_async(aiohttp_session, *request, handler1) for request in requests))
    print(results)

    await conn.close()


async def main():

    main_scheduler = AsyncScheduler()
    await main_scheduler.create_and_run_async_job('schedule_event_handler', '*/15 * * * * * *', schedule_event_handler)


if __name__ == '__main__':

    async_db_session = get_async_session()
    bybit = Bybit()

    loop = asyncio.new_event_loop()
    loop.create_task(main())

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()
