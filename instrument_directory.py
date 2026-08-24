class InstrumentDirectory:
    def __init__(self):
        self._locate_to_symbol = {}

    def register(self, stock_locate: int, symbol: str):
        self._locate_to_symbol[stock_locate] = symbol

    def symbol_for(self, stock_locate: int) -> str:
        return self._locate_to_symbol.get(stock_locate, -1)