from decimal import Decimal
from datetime import datetime, timezone, timedelta
from typing import Optional

from sqlalchemy.ext.asyncio import async_sessionmaker

from app.models import KlineHistory
from app.utils import ftod


class Iteration:
    def __init__(self, time_kline: datetime):
        self._time_kline: datetime = time_kline
        self._symbols_kline: dict = dict()

    def add_kline(self, symbol_key: str, open_price: Decimal = 0.0, high_price: Decimal = 0.0, low_price: Decimal = 0.0,
                  close_price: Decimal = 0.0, volume: Decimal = 0.0, turnover: Decimal = 0.0) -> KlineHistory:
        kline = KlineHistory()
        kline.time_kline = self._time_kline
        kline.symbol_key = symbol_key
        kline.open_price = open_price
        kline.high_price = high_price
        kline.low_price = low_price
        kline.close_price = close_price
        kline.volume = volume
        kline.turnover = turnover
        self._symbols_kline[symbol_key] = kline
        return kline

    def __getitem__(self, item) -> KlineHistory:
        return self._symbols_kline.get(item)

    @property
    def time_kline(self):
        return self._time_kline

    async def save_to_db(self, async_db_session: async_sessionmaker):
        async with async_db_session() as session:
            for symbol, kline in self._symbols_kline.items():
                session.add(kline)
                print(f'  {str(kline)}')
            await session.commit()


class IterationStack:
    __object = None

    def __new__(cls, *args, **kwargs):
        if cls.__object is None:
            cls.__object = super().__new__(cls)
        return cls.__object

    def __init__(self):
        if hasattr(self, '_time_start'):
            return
        self._time_start: datetime = datetime.utcnow()

        self._iterations: dict = dict()
        self.current_iteration: Optional[Iteration] = None

    def add_iteration(self, time_kline: datetime) -> Iteration:
        iteration = Iteration(time_kline)
        self._iterations[time_kline] = iteration
        self.current_iteration = iteration
        return iteration

    def __getitem__(self, item) -> Iteration:
        return self._iterations.get(item)

    def request_result_handler(self, status: int, result: dict) -> None:
        if not (200 <= status <= 299):
            return

        try:
            if result['retCode'] == 0:
                symbol_key = result['result']['symbol']
                time_kline = datetime.fromtimestamp(int(result['result']['list'][1][0])/1000, tz=timezone.utc)
                open_price = ftod(result['result']['list'][1][1], 9)
                high_price = ftod(result['result']['list'][1][2], 9)
                low_price = ftod(result['result']['list'][1][3], 9)
                close_price = ftod(result['result']['list'][1][4], 9)
                volume = ftod(result['result']['list'][1][5], 9)
                turnover = ftod(result['result']['list'][1][6], 9)
                iteration = self[time_kline]
                if iteration is None:
                    iteration = self.add_iteration(time_kline)
                iteration.add_kline(symbol_key, open_price, high_price, low_price, close_price, volume, turnover)

        except Exception as e:
            pass

    def garbage_collector(self):
        oldest_allowed_datetime = datetime.utcnow() - timedelta(minutes=90)
        oldest_allowed_datetime = datetime(oldest_allowed_datetime.year, oldest_allowed_datetime.month,
                                           oldest_allowed_datetime.day, oldest_allowed_datetime.hour,
                                           oldest_allowed_datetime.minute, 0, 0, tzinfo=timezone.utc)
        for kline_datetime in list(self._iterations.keys()):
            if kline_datetime < oldest_allowed_datetime:
                self._iterations.pop(kline_datetime, None)
