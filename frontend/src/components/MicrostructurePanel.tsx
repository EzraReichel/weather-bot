import type { Microstructure } from '../types'

interface Props {
  micro: Microstructure
}

function RsiGauge({ value }: { value: number }) {
  const radius = 32
  const stroke = 5
  const circumference = 2 * Math.PI * radius
  const normalizedValue = Math.max(0, Math.min(100, value))
  const dashOffset = circumference - (normalizedValue / 100) * circumference

  let color = '#a1a1aa'
  if (value < 30) color = '#22c55e'
  else if (value > 70) color = '#dc2626'

  return (
    <div className="flex flex-col items-center gap-1">
      <svg width={76} height={76} viewBox="0 0 76 76">
        <circle
          cx="38" cy="38" r={radius}
          className="gauge-ring gauge-bg"
          strokeWidth={stroke}
        />
        <circle
          cx="38" cy="38" r={radius}
          className="gauge-ring gauge-meter"
          strokeWidth={stroke}
          stroke={color}
          strokeDasharray={circumference}
          strokeDashoffset={dashOffset}
          transform="rotate(-90 38 38)"
        />
        <text
          x="38" y="35" textAnchor="middle"
          fill={color}
          fontSize="14" fontWeight="600"
          fontFamily="JetBrains Mono"
        >
          {value.toFixed(0)}
        </text>
        <text
          x="38" y="48" textAnchor="middle"
          fill="#525252"
          fontSize="8" fontWeight="500"
          textDecoration="uppercase"
        >
          RSI
        </text>
      </svg>
    </div>
  )
}

function MeterBar({ label, value, min, max, color, format }: {
  label: string
  value: number
  min: number
  max: number
  color: string
  format?: (v: number) => string
}) {
  const range = max - min
  const pct = Math.max(0, Math.min(100, ((value - min) / range) * 100))
  const displayValue = format ? format(value) : value.toFixed(2)

  return (
    <div className="space-y-0.5">
      <div className="flex items-center justify-between">
        <span className="text-[9px] text-neutral-500 uppercase tracking-wider">{label}</span>
        <span className="text-[10px] tabular-nums" style={{ color }}>{displayValue}</span>
      </div>
      <div className="meter-bar">
        <div className="meter-fill" style={{ width: `${pct}%`, backgroundColor: color }} />
      </div>
    </div>
  )
}

export function MicrostructurePanel({ micro }: Props) {
  const momColor = micro.momentum_5m >= 0 ? '#22c55e' : '#dc2626'
  const vwapColor = micro.vwap_deviation >= 0 ? '#22c55e' : '#dc2626'
  const smaColor = micro.sma_crossover >= 0 ? '#22c55e' : '#dc2626'

  return (
    <div className="flex gap-3 items-start">
      <RsiGauge value={micro.rsi} />
      <div className="flex-1 space-y-2 pt-1">
        <MeterBar
          label="Momentum"
          value={micro.momentum_5m}
          min={-0.2}
          max={0.2}
          color={momColor}
          format={v => `${v >= 0 ? '+' : ''}${v.toFixed(4)}%`}
        />
        <MeterBar
          label="VWAP Dev"
          value={micro.vwap_deviation}
          min={-0.2}
          max={0.2}
          color={vwapColor}
          format={v => `${v >= 0 ? '+' : ''}${v.toFixed(4)}%`}
        />
        <MeterBar
          label="SMA Cross"
          value={micro.sma_crossover}
          min={-0.1}
          max={0.1}
          color={smaColor}
          format={v => `${v >= 0 ? '+' : ''}${v.toFixed(4)}`}
        />
        <MeterBar
          label="Volatility"
          value={micro.volatility}
          min={0}
          max={0.1}
          color="#3b82f6"
          format={v => v.toFixed(4)}
        />
      </div>
    </div>
  )
}
