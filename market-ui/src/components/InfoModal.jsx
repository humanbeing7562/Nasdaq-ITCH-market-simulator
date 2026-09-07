export default function InfoModal({ onClose }) {
  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-content" onClick={e => e.stopPropagation()}>
        <button className="modal-close" onClick={onClose}>✕</button>

        <h2>Nasdaq ITCH 5.0 Market Data Simulator</h2>

        <p>
          A full-pipeline market data feed handler replaying real Nasdaq
          TotalView-ITCH 5.0 data as if it were live — from raw UDP multicast
          through order book reconstruction to this browser visualization.
        </p>

        <h3>What you're seeing</h3>
        <p>
          Candlestick bars and the price ladder are built from real trade and
          order book data recorded on January 30, 2019. The system replays the
          full trading day on a loop, restarting automatically when the session
          ends.
        </p>

        <h3>What makes this different</h3>
        <p>
          This is not a static dataset rendered as a chart. Every price update
          travels through the same pipeline a production feed handler would
          use — UDP multicast reception, MoldUDP64 sequence validation and gap
          recovery, a lock-free shared-memory ring buffer with gating consumers,
          per-instrument order book reconstruction, and seqlock-protected MBP-10
          snapshot publication — before reaching your browser over a WebSocket.
        </p>

        <h3>Architecture</h3>
        <div className="arch-flow">
          <span>ITCH File</span>
          <span className="arrow">→</span>
          <span>UDP Multicast</span>
          <span className="arrow">→</span>
          <span>Feed Handler</span>
          <span className="arrow">→</span>
          <span>Ring Buffer</span>
          <span className="arrow">→</span>
          <span>Book Builder</span>
          <span className="arrow">→</span>
          <span>WebSocket</span>
          <span className="arrow">→</span>
          <span>Browser</span>
        </div>

        <h3>Key design decisions</h3>
        <ul>
          <li><strong>LMAX Disruptor pattern</strong> — single-producer multi-consumer
            ring buffer with independent cursors, gating vs non-gating consumers,
            and dependency chaining between consumers</li>
          <li><strong>Zero-copy shared memory</strong> — ring buffer, MBP-10 snapshots,
            and trade buffer all use numpy structured arrays in shared memory
            with no serialization overhead</li>
          <li><strong>Seqlock-protected snapshots</strong> — book builder publishes
            top-10 levels every 200ms without blocking readers</li>
          <li><strong>Raw byte parsing</strong> — both broadcaster and processor
            dispatch on message type bytes and extract fields via struct.unpack
            at known offsets, bypassing Python object construction entirely</li>
          <li><strong>Client-side OHLCV aggregation</strong> — the server streams
            raw trades; the browser aggregates into whatever timeframe you select</li>
        </ul>

        <div className="modal-footer">
          <span>Built in Python as a learning vehicle — not a performance claim.</span>
          <a href="https://github.com/humanbeing7562/Nasdaq-ITCH-market-simulator"
             target="_blank" rel="noopener noreferrer">
            GitHub →
          </a>
        </div>
      </div>
    </div>
  );
}