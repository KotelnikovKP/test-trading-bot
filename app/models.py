from sqlalchemy import Column, String, Numeric, Boolean
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase

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

    symbol = Column(String(15), primary_key=True)
    min_leverage = Column(Numeric(19, 9), default=1.0, nullable=False)
    max_leverage = Column(Numeric(19, 9), default=25.0, nullable=False)
    leverage_step = Column(Numeric(19, 9), default=0.01, nullable=False)
    min_price = Column(Numeric(19, 9), default=0.01, nullable=False)
    max_price = Column(Numeric(19, 9), default=10000.0, nullable=False)
    tick_size = Column(Numeric(19, 9), default=0.01, nullable=False)
    min_order_qty = Column(Numeric(19, 9), default=0.1, nullable=False)
    max_order_qty = Column(Numeric(19, 9), default=1000000.0, nullable=False)
    qty_step = Column(Numeric(19, 9), default=0.1, nullable=False)
    is_active = Column(Boolean(), default=True)

    def __str__(self):
        return f'{self.symbol} ({self.is_active})'
