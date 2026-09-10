import { useState } from 'react';
import { useMarketData } from './hooks/useMarketData';
import Chart from './components/Chart';
import Ladder from './components/Ladder';
import InfoModal from './components/InfoModal';

const INTERVALS = [
  { label: '1s', value: 1 },
  { label: '5s', value: 5 },
  { label: '30s', value: 30 },
  { label: '1m', value: 60 },
  { label: '5m', value: 300 },
];

export default function App() {
  const { trades, book, symbols, connected } = useMarketData();
  const [selectedSymbol, setSelectedSymbol] = useState(null);
  const [interval, setInterval] = useState(60);
  const [showInfo, setShowInfo] = useState(true);

  // auto-select first symbol when it appears
  if (!selectedSymbol && symbols.length > 0) {
    setSelectedSymbol(symbols[0]);
  }

  return (
    <div className="app">
      <header className="toolbar">

        <div className="toolbar-left">
          <h1>Market Data Feed</h1>
          <span className={`status ${connected ? 'on' : 'off'}`}>
            {connected ? 'LIVE' : 'DISCONNECTED'}
          </span>
          <span style={{
            background: 'rgba(255, 165, 0, 0.15)',
            border: '1px solid rgba(255, 165, 0, 0.4)',
            color: '#ffa500',
            padding: '2px 8px',
            borderRadius: 4,
            fontSize: 12,
            fontFamily: 'monospace',
          }}>
            25× speed
          </span>
          <button className="info-btn" onClick={() => setShowInfo(true)}>ⓘ</button>
        </div>

        <div className="toolbar-right">
          <select
            value={selectedSymbol || ''}
            onChange={e => setSelectedSymbol(e.target.value)}
          >
            {symbols.map(s => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
          {trades.length > 0 && (
            <span style={{
              color: '#888',
              fontFamily: 'monospace',
              fontSize: 12,
            }}>
              LTP{' '}
              <span style={{ color: '#0f0', fontSize: 14 }}>
                ${trades[trades.length - 1].price.toFixed(2)}
              </span>
            </span>
          )}

          <div className="interval-picker">
            {INTERVALS.map(i => (
              <button
                key={i.value}
                className={interval === i.value ? 'active' : ''}
                onClick={() => setInterval(i.value)}
              >
                {i.label}
              </button>
            ))}
          </div>
        </div>
      </header>
      {connected && trades.length === 0 && (
        <div style={{
          textAlign: 'center',
          color: '#555',
          padding: '20px',
          fontSize: '14px',
        }}>
          Pre-market session loading — trades start at ~04:00 AM ET
        </div>
      )}
      <main className="panels">
        <div className="chart-panel">
          <Chart trades={trades} symbol={selectedSymbol} interval={interval} />
        </div>
        <div className="ladder-panel">
          <Ladder book={book} symbol={selectedSymbol} />
        </div>
      </main>
      
      {showInfo && <InfoModal onClose={() => setShowInfo(false)} />}
    </div>
  );
}