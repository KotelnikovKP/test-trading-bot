import asyncio
import json
import time
import typing
from datetime import datetime

from aiohttp import ClientSession

from config import PARALLEL_REQUESTS


async def execute_gather(*concurrency_tasks):

    semaphore = asyncio.Semaphore(PARALLEL_REQUESTS)

    async def semaphore_task(task):
        async with semaphore:
            return await task

    print(f"{datetime.utcnow()} -> ", end='')
    s = time.perf_counter()
    await asyncio.gather(*(semaphore_task(task) for task in concurrency_tasks))
    elapsed = time.perf_counter() - s
    print(f"{datetime.utcnow()} = {elapsed}")


async def request_async(session: ClientSession, method, url, data, headers, result_handler=None):

    try:
        result = None
        status = -1
        if method == 'GET':
            async with session.get(url, data=data, headers=headers, ssl=False) as response:
                status = response.status
                if 200 <= status <= 299:
                    result = await response.read()

        elif method == 'POST':
            async with session.post(url, data=data, headers=headers, ssl=False) as response:
                status = response.status
                if 200 <= status <= 299:
                    result = await response.read()
        else:
            status = -2

        if result:
            result = json.loads(result.decode())

    except Exception as e:
        result = None

    if result_handler is not None:

        try:
            if isinstance(result_handler, typing.Callable):
                result_handler(status=status, result=result)

            elif hasattr(result_handler, 'request_result_handler') and \
                    isinstance(result_handler.request_result_handler, typing.Callable):
                result_handler.request_result_handler(status=status, result=result)

            elif hasattr(result_handler, 'request_result'):
                result_handler.request_result = {'status': status, 'result': result}

            else:
                print(status)
                print(result)

        except Exception as e:
            print(str(e))
            print(status)
            print(result)

    else:
        print(status)
        print(result)
