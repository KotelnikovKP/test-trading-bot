import asyncio
from datetime import datetime

from app.scheduler import AsyncScheduler


async def schedule_event_handler():
    print(f'{datetime.utcnow()}: Schedule even has done!')


async def main():

    main_scheduler = AsyncScheduler()
    await main_scheduler.create_and_run_async_job('schedule_event_handler', '*/15 * * * * * *', schedule_event_handler)


if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    loop.create_task(main())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()
