import asyncio
import json

import aiohttp
from sqlalchemy import update, select

from app.config import SYMBOLS, BASE_SYMBOLS, LIMIT_PER_HOST, LIMIT, TTL_DNS_CACHE
from app.bybit import Bybit
from app.models import get_async_session, Symbol
from app.utils import ftod


async def main():

    # Get symbols info from Bybit
    conn = aiohttp.TCPConnector(limit_per_host=LIMIT_PER_HOST, limit=LIMIT, ttl_dns_cache=TTL_DNS_CACHE)
    aiohttp_session = aiohttp.ClientSession(connector=conn)

    bybit = Bybit()

    bybit_symbols = {}
    try:
        method, url, data, headers = bybit.get_instruments_info(category='linear')
        async with aiohttp_session.get(url, data=data, headers=headers, ssl=False) as response:
            obj = await response.read()
            bybit_symbols = json.loads(obj.decode())
    except Exception as e:
        print(str(e))

    await conn.close()

    async_db_session = get_async_session()

    async with async_db_session() as session:

        # Clear the activate flag of existing symbols
        await session.execute(
            update(Symbol).
            where(Symbol.symbol.notin_(BASE_SYMBOLS)).
            values(is_active=False)
        )

        # Update/create each symbol of list SYMBOLS in db with the newest parameters values from Bybit
        if bybit_symbols:
            for s in bybit_symbols['result']['list']:
                if s['symbol'] in SYMBOLS:
                    db_symbol = await session.execute(
                        select(Symbol).
                        where(Symbol.symbol == s['symbol'])
                    )
                    db_symbol = db_symbol.scalars().one_or_none()
                    if db_symbol:
                        db_symbol.min_leverage = ftod(s['leverageFilter']['minLeverage'], 9)
                        db_symbol.max_leverage = ftod(s['leverageFilter']['maxLeverage'], 9)
                        db_symbol.leverage_step = ftod(s['leverageFilter']['leverageStep'], 9)
                        db_symbol.min_price = ftod(s['priceFilter']['minPrice'], 9)
                        db_symbol.max_price = ftod(s['priceFilter']['maxPrice'], 9)
                        db_symbol.tick_size = ftod(s['priceFilter']['tickSize'], 9)
                        db_symbol.min_order_qty = ftod(s['lotSizeFilter']['minOrderQty'], 9)
                        db_symbol.max_order_qty = ftod(s['lotSizeFilter']['maxOrderQty'], 9)
                        db_symbol.qty_step = ftod(s['lotSizeFilter']['qtyStep'], 9)
                        db_symbol.is_active = True
                    else:
                        db_symbol = Symbol(
                            symbol=s['symbol'],
                            min_leverage=ftod(s['leverageFilter']['minLeverage'], 9),
                            max_leverage=ftod(s['leverageFilter']['maxLeverage'], 9),
                            leverage_step=ftod(s['leverageFilter']['leverageStep'], 9),
                            min_price=ftod(s['priceFilter']['minPrice'], 9),
                            max_price=ftod(s['priceFilter']['maxPrice'], 9),
                            tick_size=ftod(s['priceFilter']['tickSize'], 9),
                            min_order_qty=ftod(s['lotSizeFilter']['minOrderQty'], 9),
                            max_order_qty=ftod(s['lotSizeFilter']['maxOrderQty'], 9),
                            qty_step=ftod(s['lotSizeFilter']['qtyStep'], 9),
                            is_active=True
                        )
                    session.add(db_symbol)

        await session.commit()


if __name__ == '__main__':
    asyncio.run(main())
