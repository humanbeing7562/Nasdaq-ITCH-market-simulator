export default function Ladder({ book, symbol }) {
  const data = book[symbol];

  if (!data || (data.bids.length === 0 && data.asks.length === 0)) {
    return <div className="ladder empty">Waiting for book data...</div>;
  }

  const asks = [...data.asks].reverse();
  const bids = data.bids;

  const maxQty = Math.max(
    ...data.bids.map(l => l.qty),
    ...data.asks.map(l => l.qty),
    1
  );

  return (
    <div className="ladder">
      <div className="ladder-header">
        <span>Qty</span>
        <span>Price</span>
        <span>Qty</span>
      </div>

      {asks.map((level, i) => (
        <div key={`a${i}`} className="ladder-row ask">
          <span className="qty-cell"></span>
          <span className="price-cell ask-price">
            {level.price.toFixed(4)}
          </span>
          <span className="qty-cell">
            <span
              className="qty-bar ask-bar"
              style={{ width: `${(level.qty / maxQty) * 100}%` }}
            />
            <span className="qty-text">{level.qty}</span>
          </span>
        </div>
      ))}

      {bids.map((level, i) => (
        <div key={`b${i}`} className="ladder-row bid">
          <span className="qty-cell">
            <span
              className="qty-bar bid-bar"
              style={{ width: `${(level.qty / maxQty) * 100}%` }}
            />
            <span className="qty-text">{level.qty}</span>
          </span>
          <span className="price-cell bid-price">
            {level.price.toFixed(4)}
          </span>
          <span className="qty-cell"></span>
        </div>
      ))}
    </div>
  );
}