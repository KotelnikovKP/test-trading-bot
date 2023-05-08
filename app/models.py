from datetime import datetime
from decimal import Decimal
from typing import List

from sqlalchemy import String, Numeric, Boolean, DateTime, ForeignKey, PrimaryKeyConstraint, Integer
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from app.config import DATABASE_HOST, DATABASE_NAME, DATABASE_PORT, DATABASE_USER, DATABASE_PASSWORD


class Base(DeclarativeBase):
    pass


def get_engine():
    return create_async_engine('postgresql+asyncpg://' +
                               DATABASE_USER + ':' +
                               DATABASE_PASSWORD + '@' +
                               DATABASE_HOST + ':' +
                               DATABASE_PORT + '/' +
                               DATABASE_NAME)


def get_async_session():
    engine = get_engine()
    session = async_sessionmaker(engine, expire_on_commit=False)
    return session


class Symbol(Base):

    __tablename__ = 'symbol'

    symbol: Mapped[str] = mapped_column(String(15), primary_key=True, index=True)
    min_leverage: Mapped[Decimal] = mapped_column(Numeric(19, 9), default=1.0, nullable=False)
    max_leverage: Mapped[Decimal] = mapped_column(Numeric(19, 9), default=25.0, nullable=False)
    leverage_step: Mapped[Decimal] = mapped_column(Numeric(19, 9), default=0.01, nullable=False)
    min_price: Mapped[Decimal] = mapped_column(Numeric(19, 9), default=0.01, nullable=False)
    max_price: Mapped[Decimal] = mapped_column(Numeric(19, 9), default=10000.0, nullable=False)
    tick_size: Mapped[Decimal] = mapped_column(Numeric(19, 9), default=0.01, nullable=False)
    min_order_qty: Mapped[Decimal] = mapped_column(Numeric(19, 9), default=0.1, nullable=False)
    max_order_qty: Mapped[Decimal] = mapped_column(Numeric(19, 9), default=1000000.0, nullable=False)
    qty_step: Mapped[Decimal] = mapped_column(Numeric(19, 9), default=0.1, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean(), default=True)
    kline_history: Mapped[List['KlineHistory']] = relationship(back_populates='symbol', cascade='all, delete-orphan')

    def __str__(self):
        return f'{self.symbol} ({self.is_active})'


class KlineHistory(Base):

    __tablename__ = 'kline_history'

    time_kline: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    symbol_key: Mapped[str] = mapped_column(ForeignKey('symbol.symbol'), nullable=False, index=True)
    symbol: Mapped['Symbol'] = relationship(back_populates='kline_history')
    open_price: Mapped[Decimal] = mapped_column(Numeric(19, 9), default=0.0, nullable=False)
    high_price: Mapped[Decimal] = mapped_column(Numeric(19, 9), default=0.0, nullable=False)
    low_price: Mapped[Decimal] = mapped_column(Numeric(19, 9), default=0.0, nullable=False)
    close_price: Mapped[Decimal] = mapped_column(Numeric(19, 9), default=0.0, nullable=False)
    volume: Mapped[Decimal] = mapped_column(Numeric(19, 9), default=0.0, nullable=False)
    turnover: Mapped[Decimal] = mapped_column(Numeric(19, 9), default=0.0, nullable=False)

    max_price: Mapped[Decimal] = mapped_column(Numeric(19, 9), nullable=True)
    delta_to_max: Mapped[Decimal] = mapped_column(Numeric(19, 9), nullable=True)
    delta_to_max_in_percent: Mapped[Decimal] = mapped_column(Numeric(19, 9), nullable=True)
    time_since_max: Mapped[int] = mapped_column(Integer, nullable=True)

    min_price: Mapped[Decimal] = mapped_column(Numeric(19, 9), nullable=True)
    delta_to_min: Mapped[Decimal] = mapped_column(Numeric(19, 9), nullable=True)
    delta_to_min_in_percent: Mapped[Decimal] = mapped_column(Numeric(19, 9), nullable=True)
    time_since_min: Mapped[int] = mapped_column(Integer, nullable=True)

    btc_impact_rate: Mapped[Decimal] = mapped_column(Numeric(19, 9), nullable=True)

    is_growth_over_1_percent: Mapped[bool] = mapped_column(Boolean(), default=False)
    is_decline_over_1_percent: Mapped[bool] = mapped_column(Boolean(), default=False)

    __table_args__ = (
        PrimaryKeyConstraint('symbol_key', 'time_kline', name='key_time'),
    )

    def __str__(self):
        return f'{self.time_kline} - {self.symbol_key}: (' \
               f'O={self.open_price}, H={self.high_price}, ' \
               f'L={self.low_price}, C={self.close_price}, ' \
               f'V={self.volume}, T={self.turnover}, ' \
               f'B={self.btc_impact_rate})'
