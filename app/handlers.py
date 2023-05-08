from decimal import Decimal
from datetime import datetime, timezone, timedelta
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.config import TRACKING_PERIOD, ALARM_THRESHOLD, BTC_IMPACT_THRESHOLD
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

    def add_existing_kline(self, kline: KlineHistory) -> None:
        self._symbols_kline[kline.symbol_key] = kline

    def __getitem__(self, item) -> KlineHistory:
        return self._symbols_kline.get(item)

    @property
    def time_kline(self):
        return self._time_kline

    @property
    def symbols_kline(self):
        return self._symbols_kline

    def __len__(self):
        return len(self._symbols_kline)

    async def save_to_db(self, async_db_session: async_sessionmaker) -> None:
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

    async def get_kline_history(self, async_db_session: async_sessionmaker):
        oldest_allowed_datetime = datetime.utcnow() - timedelta(minutes=TRACKING_PERIOD + 1)
        oldest_allowed_datetime = datetime(oldest_allowed_datetime.year, oldest_allowed_datetime.month,
                                           oldest_allowed_datetime.day, oldest_allowed_datetime.hour,
                                           oldest_allowed_datetime.minute, 0, 0, tzinfo=timezone.utc)

        async with async_db_session() as session:
            kline_history = await session.execute(
                select(KlineHistory).
                where(KlineHistory.time_kline > oldest_allowed_datetime)
            )
            for kline in kline_history.scalars():
                iteration = self[kline.time_kline]
                if iteration is None:
                    iteration = self.add_iteration(kline.time_kline)
                iteration.add_existing_kline(kline)

    def add_iteration(self, time_kline: datetime) -> Iteration:
        iteration = Iteration(time_kline)
        self._iterations[time_kline] = iteration
        self.current_iteration = iteration
        return iteration

    def __getitem__(self, item) -> Iteration:
        return self._iterations.get(item)

    def __len__(self):
        return len(self._iterations)

    def request_result_handler(self, status: int, result: dict) -> None:
        if not (200 <= status <= 299):
            return

        try:
            if result['retCode'] == 0:
                symbol_key = result['result']['symbol']
                time_kline = datetime.fromtimestamp(int(result['result']['list'][1][0]) / 1000, tz=timezone.utc)
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

    def garbage_collector(self) -> None:
        oldest_allowed_datetime = datetime.utcnow() - timedelta(minutes=TRACKING_PERIOD + 5)
        oldest_allowed_datetime = datetime(oldest_allowed_datetime.year, oldest_allowed_datetime.month,
                                           oldest_allowed_datetime.day, oldest_allowed_datetime.hour,
                                           oldest_allowed_datetime.minute, 0, 0, tzinfo=timezone.utc)
        for kline_datetime in list(self._iterations.keys()):
            if kline_datetime < oldest_allowed_datetime:
                self._iterations.pop(kline_datetime, None)

    def _get_max_min_in_period(
            self, symbol_key: str, end_time: datetime
    ) -> tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal], Optional[int],
    Optional[Decimal], Optional[Decimal], Optional[Decimal], Optional[int]]:
        def minutes_diff(finish_time: datetime, start_time: datetime) -> int:
            return int((finish_time.timestamp() - start_time.timestamp()) / 60)

        end_time = datetime(year=end_time.year, month=end_time.month, day=end_time.day, hour=end_time.hour,
                            minute=end_time.minute, second=0, microsecond=0, tzinfo=timezone.utc)
        max_price = delta_to_max = delta_to_max_in_percent = time_since_max = None
        min_price = delta_to_min = delta_to_min_in_percent = time_since_min = None

        kline = None
        iter_time = end_time
        while kline is None and iter_time is not None:
            iteration = self[iter_time]
            if iteration:
                kline = iteration[symbol_key]
            iter_time = iter_time - timedelta(minutes=1)
            if iter_time < end_time - timedelta(minutes=TRACKING_PERIOD):
                iter_time = None

        if kline:
            current_price = kline.close_price
            max_price = kline.high_price
            delta_to_max = current_price - max_price
            delta_to_max_in_percent = delta_to_max / max_price
            time_since_max = minutes_diff(end_time, kline.time_kline)
            min_price = kline.low_price
            delta_to_min = current_price - min_price
            delta_to_min_in_percent = delta_to_min / min_price
            time_since_min = minutes_diff(end_time, kline.time_kline)

            for time_kline, iteration in self._iterations.items():
                if end_time - timedelta(minutes=TRACKING_PERIOD) <= time_kline <= end_time:
                    kline = iteration[symbol_key]
                    if kline:
                        if kline.high_price > max_price:
                            max_price = kline.high_price
                            delta_to_max = current_price - max_price
                            delta_to_max_in_percent = delta_to_max / max_price
                            time_since_max = minutes_diff(end_time, time_kline)
                        if kline.low_price < min_price:
                            min_price = kline.low_price
                            delta_to_min = current_price - min_price
                            delta_to_min_in_percent = delta_to_min / min_price
                            time_since_min = minutes_diff(end_time, time_kline)

        return max_price, delta_to_max, delta_to_max_in_percent, time_since_max, \
            min_price, delta_to_min, delta_to_min_in_percent, time_since_min

    def _get_btc_impact_rate(self, kline: KlineHistory, btc_kline: KlineHistory) -> Optional[Decimal]:

        def get_normal_value(value: Decimal, min_value: Decimal, max_value: Decimal) -> Decimal:
            return ftod((value - min_value) / (max_value - min_value), 9)

        end_time = datetime(year=kline.time_kline.year, month=kline.time_kline.month, day=kline.time_kline.day,
                            hour=kline.time_kline.hour, minute=kline.time_kline.minute, second=0, microsecond=0,
                            tzinfo=timezone.utc)
        symbol_key = kline.symbol_key
        btc_symbol_key = btc_kline.symbol_key

        btc_impact_rate = None

        linear_deviation_sum = ftod(0.0, 9)
        linear_deviation_count = 0

        max_price = kline.max_price
        min_price = kline.min_price

        btc_max_price = btc_kline.max_price
        btc_min_price = btc_kline.min_price

        for time_kline, iteration in self._iterations.items():
            if end_time - timedelta(minutes=TRACKING_PERIOD) <= time_kline <= end_time:
                kline = iteration[symbol_key]
                btc_kline = iteration[btc_symbol_key]
                if kline and btc_kline:
                    linear_deviation_sum += \
                        abs(get_normal_value(kline.close_price, min_price, max_price) -
                            get_normal_value(btc_kline.close_price, btc_min_price, btc_max_price))
                    linear_deviation_count += 1

        if linear_deviation_count:
            btc_impact_rate = ftod(1 - linear_deviation_sum / linear_deviation_count, 9)

        return btc_impact_rate

    def calculate_indicators(self, iteration: Iteration) -> None:
        btc_symbol_key = 'BTCUSDT'
        btc_kline = iteration[btc_symbol_key]
        if btc_kline:
            btc_kline.max_price, btc_kline.delta_to_max, \
                btc_kline.delta_to_max_in_percent, btc_kline.time_since_max, \
                btc_kline.min_price, btc_kline.delta_to_min, \
                btc_kline.delta_to_min_in_percent, btc_kline.time_since_min = \
                self._get_max_min_in_period(btc_symbol_key, btc_kline.time_kline)
            btc_kline.btc_impact_rate = ftod(1.0, 9)

        for symbol_key, kline in iteration.symbols_kline.items():
            if symbol_key != btc_symbol_key:
                kline.max_price, kline.delta_to_max, \
                    kline.delta_to_max_in_percent, kline.time_since_max, \
                    kline.min_price, kline.delta_to_min, \
                    kline.delta_to_min_in_percent, kline.time_since_min = \
                    self._get_max_min_in_period(symbol_key, kline.time_kline)
                if btc_kline:
                    kline.btc_impact_rate = self._get_btc_impact_rate(kline, btc_kline)
                else:
                    kline.btc_impact_rate = ftod(0.0, 9)

    def _announce_victory(self, kline: KlineHistory) -> None:
        print('-------------------------------------------------------------------------------------------------------')
        print(f'{datetime.utcnow()} УРА!!!')
        if kline.is_growth_over_1_percent:
            print(f"  Цена фьючерса {kline.symbol_key} выросла на {abs(kline.delta_to_min_in_percent) * 100:.2f} %")
        if kline.is_decline_over_1_percent:
            print(f"  Цена фьючерса {kline.symbol_key} упала на {abs(kline.delta_to_max_in_percent) * 100:.2f} %")
        print('-------------------------------------------------------------------------------------------------------')

    def make_decision(self, iteration: Iteration) -> None:
        for symbol_key, kline in iteration.symbols_kline.items():
            if abs(kline.delta_to_min_in_percent) >= ALARM_THRESHOLD and kline.btc_impact_rate <= BTC_IMPACT_THRESHOLD:
                kline.is_growth_over_1_percent = True
                self._announce_victory(kline)
            if abs(kline.delta_to_max_in_percent) >= ALARM_THRESHOLD and kline.btc_impact_rate <= BTC_IMPACT_THRESHOLD:
                kline.is_decline_over_1_percent = True
                self._announce_victory(kline)
