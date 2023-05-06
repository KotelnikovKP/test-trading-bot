from decimal import Decimal


def ftod(value, precision=15):
    """
    Функция приведение значения к Decimal с заданной точностью
    """
    if value is None:
        value = 0.00
    return Decimal(value).quantize(Decimal(10) ** -precision)
