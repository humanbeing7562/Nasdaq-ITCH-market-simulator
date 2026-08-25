class Event:
    def __init__(self, action, ts_event, ts_recv, sequence, order_id, quantity, side, instrument_id, price):
        self.action = action
        self.ts_event = ts_event
        self.ts_recv = ts_recv
        self.sequence = sequence
        self.order_id = order_id
        self.quantity = quantity
        self.side = side
        self.instrument_id = instrument_id
        self.price = price


    def __str__(self):
        return f"{self.action}: {self.ts_event} {self.ts_recv} {self.sequence} {self.order_id} {self.quantity} {self.side} {self.instrument_id} {self.price}"


    