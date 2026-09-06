import { useEffect, useRef } from 'react';
import { createChart, CandlestickSeries, HistogramSeries } from 'lightweight-charts';

function aggregateBars(trades, symbol, intervalSec) {
  const filtered = trades.filter(t => t.symbol === symbol);
  if (filtered.length === 0) return [];
  const BASE_DATE = new Date('2019-01-30T00:00:00Z').getTime() / 1000;
  const bars = {};
  for (const t of filtered) {
    // bucket per second
    const timeSec = BASE_DATE + Math.floor(t.time / 1e9);
    const bucket = Math.floor(timeSec / intervalSec) * intervalSec;

    if (!bars[bucket]) {
      bars[bucket] = {
        time: bucket,
        open: t.price,
        high: t.price,
        low: t.price,
        close: t.price,
        volume: t.quantity,
      };
    } else {
      const bar = bars[bucket];
      bar.high = Math.max(bar.high, t.price);
      bar.low = Math.min(bar.low, t.price);
      bar.close = t.price;
      bar.volume += t.quantity;
    }
  }
  

  return Object.values(bars).sort((a, b) => a.time - b.time);
}

export default function Chart({ trades, symbol, interval }) {
  const containerRef = useRef(null);
  const chartRef = useRef(null);
  const seriesRef = useRef(null);
  const volumeRef = useRef(null);

  useEffect(() => {
    
    if (!containerRef.current) return;

    const chart = createChart(containerRef.current, {
      width: containerRef.current.clientWidth,
      height: containerRef.current.clientHeight,
      layout: {
        background: { color: '#1a1a2e' },
        textColor: '#e0e0e0',
      },
      grid: {
        vertLines: { color: '#2a2a3e' },
        horzLines: { color: '#2a2a3e' },
      },
      timeScale: {
        timeVisible: true,
        secondsVisible: true,
      },
    });

    const series = chart.addSeries(CandlestickSeries, {
      upColor: '#00c853',
      downColor: '#ff1744',
      wickUpColor: '#00c853',
      wickDownColor: '#ff1744',
      borderVisible: false,
    });

    chartRef.current = chart;
    seriesRef.current = series;
    const volumeSeries = chart.addSeries(HistogramSeries, {
      color: '#4a4a6a',
      priceFormat: { type: 'volume' },
      priceScaleId: 'volume',
    });

      chart.priceScale('volume').applyOptions({
      scaleMargins: { top: 0.8, bottom: 0 },
    });

    volumeRef.current = volumeSeries;


    chart.subscribeCrosshairMove((param) => {
        if (!param || !param.time) return;
        const data = param.seriesData.get(series);
        const vol = param.seriesData.get(volumeSeries);
        if (!data) return;

        const legend = document.getElementById('chart-legend');
        if (legend) {
            legend.innerHTML = `
            <span>O <b>${data.open.toFixed(4)}</b></span>
            <span>H <b>${data.high.toFixed(4)}</b></span>
            <span>L <b>${data.low.toFixed(4)}</b></span>
            <span>C <b>${data.close.toFixed(4)}</b></span>
            <span>V <b>${vol ? vol.value.toLocaleString() : '—'}</b></span>
            `;
        }
    });
    const handleResize = () => {
      chart.applyOptions({
        width: containerRef.current.clientWidth,
        height: containerRef.current.clientHeight,
      });
    };
    window.addEventListener('resize', handleResize);

    return () => {
      window.removeEventListener('resize', handleResize);
      chart.remove();
    };
  }, []);

  
  useEffect(() => {
    if (!seriesRef.current || !symbol) return;
    const bars = aggregateBars(trades, symbol, interval);
    seriesRef.current.setData(bars);
    if (volumeRef.current) {
      const volumeBars = bars.map(b => ({
        time: b.time,
        value: b.volume,
        color: b.close >= b.open ? 'rgba(0,200,83,0.3)' : 'rgba(255,23,68,0.3)',
      }));
      volumeRef.current.setData(volumeBars);
    }
    if (bars.length > 0 && chartRef.current) {
        chartRef.current.timeScale();
    }
  }, [trades, symbol, interval]);

  return (
  <div ref={containerRef} style={{ width: '100%', height: '100%', position: 'relative' }}>
    <div id="chart-legend" style={{
      position: 'absolute',
      top: 8,
      left: 8,
      zIndex: 10,
      fontSize: '12px',
      color: '#e0e0e0',
      display: 'flex',
      gap: '12px',
      fontFamily: 'inherit',
    }} />
  </div>
  );
}