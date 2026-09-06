import { useState, useEffect, useRef } from 'react';

const PRICE_SCALE = 10000;

export function useMarketData(url = 'ws://localhost:8765') {
  const [book, setBook] = useState({});
  const [trades, setTrades] = useState([]);
  const [symbols, setSymbols] = useState([]);
  const [connected, setConnected] = useState(false);
  const wsRef = useRef(null);
  const symbolsRef = useRef(new Set());

  useEffect(() => {
    const ws = new WebSocket(url);
    wsRef.current = ws;

    ws.onopen = () => setConnected(true);
    ws.onclose = () => setConnected(false);

    ws.onmessage = (event) => {
      console.log('HOOK received:', event.data.slice(0, 200));
      const msg = JSON.parse(event.data);

      if (msg.type === 'trade') {
        const trade = {
          symbol: msg.symbol,
          price: msg.price / PRICE_SCALE,
          quantity: msg.quantity,
          time: msg.ts_event,
        };
        setTrades(prev => [...prev, trade]);

        if (!symbolsRef.current.has(msg.symbol)) {
          symbolsRef.current.add(msg.symbol);
          setSymbols(Array.from(symbolsRef.current).sort());
        }
      } else if (msg.type === 'book_update') {
        const updated = {};
        for (const b of msg.books) {
          updated[b.symbol] = {
            bids: b.bids.map(l => ({
              price: l.price / PRICE_SCALE,
              qty: l.qty,
            })),
            asks: b.asks.map(l => ({
              price: l.price / PRICE_SCALE,
              qty: l.qty,
            })),
          };
        }
        setBook(prev => ({ ...prev, ...updated }));
      } else if (msg.type === 'trade_batch') {
          const newTrades = msg.trades.map(t => ({
            symbol: t.symbol,
            price: t.price / PRICE_SCALE,
            quantity: t.quantity,
            time: t.ts_event,
          }));
          setTrades(prev => [...prev, ...newTrades]);

          let changed = false;
          for (const t of msg.trades) {
            if (!symbolsRef.current.has(t.symbol)) {
              symbolsRef.current.add(t.symbol);
              changed = true;
            }
          }
          if (changed) setSymbols(Array.from(symbolsRef.current).sort());
      }
    };

    return () => ws.close();
  }, [url]);

  return { trades, book, symbols, connected };
}