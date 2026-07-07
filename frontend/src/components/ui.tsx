'use client';

import {
  ReactNode,
  useState,
  InputHTMLAttributes,
  TextareaHTMLAttributes,
  SelectHTMLAttributes,
  ButtonHTMLAttributes,
} from 'react';

// ── Utility ──────────────────────────────────────────────────────────────────
export function cn(...classes: (string | undefined | false | null)[]): string {
  return classes.filter(Boolean).join(' ');
}

export function fmt(n: number, decimals = 2): string {
  return n.toLocaleString('en-US', { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
}

export function fmtPct(n: number): string {
  return `${n >= 0 ? '+' : ''}${fmt(n)}%`;
}

export function fmtUSD(n: number): string {
  return n.toLocaleString('en-US', { style: 'currency', currency: 'USD' });
}

// ── Spinner ───────────────────────────────────────────────────────────────────
export function Spinner({ size = 'md' }: { size?: 'sm' | 'md' | 'lg' }) {
  const s = size === 'sm' ? 'w-4 h-4' : size === 'lg' ? 'w-8 h-8' : 'w-5 h-5';
  return (
    <svg className={cn(s, 'animate-spin text-blue-500 flex-shrink-0')} fill="none" viewBox="0 0 24 24">
      <circle className="opacity-20" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
      <path className="opacity-80" fill="currentColor" d="M4 12a8 8 0 018-8v4a4 4 0 00-4 4H4z" />
    </svg>
  );
}

// ── Badge ─────────────────────────────────────────────────────────────────────
type BadgeVariant = 'green' | 'red' | 'yellow' | 'blue' | 'gray' | 'orange' | 'purple';

const BADGE: Record<BadgeVariant, string> = {
  green:  'bg-green-950 text-green-400 border border-green-800/60',
  red:    'bg-red-950 text-red-400 border border-red-800/60',
  yellow: 'bg-amber-950 text-amber-400 border border-amber-800/60',
  orange: 'bg-orange-950 text-orange-400 border border-orange-800/60',
  blue:   'bg-blue-950 text-blue-400 border border-blue-800/60',
  gray:   'bg-slate-800 text-slate-400 border border-slate-700/60',
  purple: 'bg-purple-950 text-purple-400 border border-purple-800/60',
};

export function Badge({ children, variant = 'gray' }: { children: ReactNode; variant?: BadgeVariant }) {
  return (
    <span className={cn('inline-flex items-center px-2 py-0.5 rounded text-xs font-medium', BADGE[variant])}>
      {children}
    </span>
  );
}

// ── Button ────────────────────────────────────────────────────────────────────
type BtnVariant = 'primary' | 'secondary' | 'danger' | 'ghost';
type BtnSize = 'xs' | 'sm' | 'md';

const BTN_VAR: Record<BtnVariant, string> = {
  primary:   'bg-blue-600 hover:bg-blue-500 text-white border border-blue-600',
  secondary: 'bg-slate-800 hover:bg-slate-700 text-slate-200 border border-slate-700',
  danger:    'bg-red-950 hover:bg-red-900 text-red-400 border border-red-800/60',
  ghost:     'bg-transparent hover:bg-slate-800 text-slate-400 border border-transparent',
};
const BTN_SZ: Record<BtnSize, string> = {
  xs: 'px-2 py-1 text-xs gap-1',
  sm: 'px-2.5 py-1.5 text-xs gap-1.5',
  md: 'px-3.5 py-2 text-sm gap-2',
};

interface BtnProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: BtnVariant;
  size?: BtnSize;
  loading?: boolean;
}

export function Button({ children, variant = 'primary', size = 'md', loading, disabled, className, ...props }: BtnProps) {
  return (
    <button
      className={cn(
        'inline-flex items-center justify-center rounded-lg font-medium transition-colors',
        'disabled:opacity-50 disabled:cursor-not-allowed cursor-pointer',
        BTN_VAR[variant], BTN_SZ[size], className,
      )}
      disabled={disabled || loading}
      {...props}
    >
      {loading && <Spinner size="sm" />}
      {children}
    </button>
  );
}

// ── Form inputs ───────────────────────────────────────────────────────────────
const INPUT_BASE =
  'w-full rounded-lg bg-slate-900 border border-slate-700 text-slate-200 ' +
  'placeholder-slate-500 px-3 py-2 text-sm focus:outline-none focus:ring-1 ' +
  'focus:ring-blue-500 focus:border-blue-500 transition-colors';

interface LabeledProps { label?: string; error?: string; hint?: string }

export function Label({ children, htmlFor }: { children: ReactNode; htmlFor?: string }) {
  return (
    <label htmlFor={htmlFor} className="block text-xs font-medium text-slate-400 mb-1.5">
      {children}
    </label>
  );
}

export function Input({ label, error, hint, className, ...props }: LabeledProps & InputHTMLAttributes<HTMLInputElement>) {
  return (
    <div className="w-full">
      {label && <Label htmlFor={props.id as string}>{label}</Label>}
      <input className={cn(INPUT_BASE, error && 'border-red-600 focus:ring-red-500', className)} {...props} />
      {hint && !error && <p className="mt-1 text-xs text-slate-500">{hint}</p>}
      {error && <p className="mt-1 text-xs text-red-400">{error}</p>}
    </div>
  );
}

export function Textarea({
  label, error, hint, rows = 3, className, ...props
}: LabeledProps & TextareaHTMLAttributes<HTMLTextAreaElement> & { rows?: number }) {
  return (
    <div className="w-full">
      {label && <Label htmlFor={props.id as string}>{label}</Label>}
      <textarea rows={rows} className={cn(INPUT_BASE, 'resize-none', error && 'border-red-600', className)} {...props} />
      {hint && !error && <p className="mt-1 text-xs text-slate-500">{hint}</p>}
      {error && <p className="mt-1 text-xs text-red-400">{error}</p>}
    </div>
  );
}

export function Select({
  label, error, hint, children, className, ...props
}: LabeledProps & SelectHTMLAttributes<HTMLSelectElement>) {
  return (
    <div className="w-full">
      {label && <Label htmlFor={props.id as string}>{label}</Label>}
      <select className={cn(INPUT_BASE, 'cursor-pointer', error && 'border-red-600', className)} {...props}>
        {children}
      </select>
      {hint && !error && <p className="mt-1 text-xs text-slate-500">{hint}</p>}
      {error && <p className="mt-1 text-xs text-red-400">{error}</p>}
    </div>
  );
}

// ── Card ──────────────────────────────────────────────────────────────────────
export function Card({ children, className }: { children: ReactNode; className?: string }) {
  return (
    <div className={cn('bg-slate-900 border border-slate-800 rounded-xl overflow-hidden', className)}>
      {children}
    </div>
  );
}

export function CardHeader({
  title, subtitle, action,
}: { title: string; subtitle?: string; action?: ReactNode }) {
  return (
    <div className="flex items-start justify-between px-5 py-4 border-b border-slate-800">
      <div>
        <p className="text-sm font-semibold text-slate-200">{title}</p>
        {subtitle && <p className="text-xs text-slate-500 mt-0.5">{subtitle}</p>}
      </div>
      {action && <div className="ml-4 flex-shrink-0">{action}</div>}
    </div>
  );
}

export function CardContent({ children, className }: { children: ReactNode; className?: string }) {
  return <div className={cn('px-5 py-4', className)}>{children}</div>;
}

// ── Stat card ─────────────────────────────────────────────────────────────────
interface StatProps {
  label: string;
  value: string | number;
  sub?: string;
  up?: boolean;
  color?: 'blue' | 'green' | 'red' | 'yellow' | 'default';
}

const STAT_CLR: Record<NonNullable<StatProps['color']>, string> = {
  blue:    'text-blue-400',
  green:   'text-green-400',
  red:     'text-red-400',
  yellow:  'text-amber-400',
  default: 'text-slate-100',
};

export function StatCard({ label, value, sub, up, color = 'default' }: StatProps) {
  return (
    <Card>
      <CardContent className="py-4">
        <p className="text-xs text-slate-500 uppercase tracking-wider">{label}</p>
        <p className={cn('text-2xl font-bold mt-1.5 tabular-nums', STAT_CLR[color])}>{value}</p>
        {sub !== undefined && (
          <p className={cn('text-xs mt-1', up === true ? 'text-green-400' : up === false ? 'text-red-400' : 'text-slate-500')}>
            {up === true ? '▲ ' : up === false ? '▼ ' : ''}{sub}
          </p>
        )}
      </CardContent>
    </Card>
  );
}

// ── Alert ─────────────────────────────────────────────────────────────────────
type AlertVariant = 'error' | 'success' | 'info' | 'warning';

const ALERT_STY: Record<AlertVariant, string> = {
  error:   'bg-red-950/60 border-red-800/50 text-red-300',
  success: 'bg-green-950/60 border-green-800/50 text-green-300',
  info:    'bg-blue-950/60 border-blue-800/50 text-blue-300',
  warning: 'bg-amber-950/60 border-amber-800/50 text-amber-300',
};

export function Alert({
  message, variant = 'error', onClose,
}: { message: string; variant?: AlertVariant; onClose?: () => void }) {
  if (!message) return null;
  return (
    <div className={cn('flex items-start gap-2 rounded-lg border px-4 py-3 text-sm', ALERT_STY[variant])}>
      <span className="flex-1">{message}</span>
      {onClose && (
        <button onClick={onClose} className="opacity-60 hover:opacity-100 transition-opacity flex-shrink-0 leading-none">
          ✕
        </button>
      )}
    </div>
  );
}

// ── Table ─────────────────────────────────────────────────────────────────────
export function Table({ children }: { children: ReactNode }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">{children}</table>
    </div>
  );
}
export function Thead({ children }: { children: ReactNode }) {
  return <thead className="border-b border-slate-800">{children}</thead>;
}
export function Tbody({ children }: { children: ReactNode }) {
  return <tbody className="divide-y divide-slate-800/50">{children}</tbody>;
}
export function Tr({ children, className }: { children?: ReactNode; className?: string }) {
  return <tr className={cn('transition-colors hover:bg-slate-800/30', className)}>{children}</tr>;
}
export function Th({ children, className }: { children?: ReactNode; className?: string }) {
  return (
    <th className={cn('text-left text-xs font-medium text-slate-500 uppercase tracking-wider px-4 py-3', className)}>
      {children}
    </th>
  );
}
export function Td({ children, className }: { children?: ReactNode; className?: string }) {
  return <td className={cn('px-4 py-3 text-slate-300', className)}>{children}</td>;
}

// ── Tabs ──────────────────────────────────────────────────────────────────────
export function Tabs({
  tabs, active, onSelect,
}: { tabs: { id: string; label: string }[]; active: string; onSelect: (id: string) => void }) {
  return (
    <div className="flex items-center gap-1 bg-slate-800/50 border border-slate-700/50 rounded-lg p-1 w-fit">
      {tabs.map(t => (
        <button
          key={t.id}
          onClick={() => onSelect(t.id)}
          className={cn(
            'px-3 py-1.5 rounded-md text-sm font-medium transition-colors',
            active === t.id
              ? 'bg-blue-600 text-white shadow-sm'
              : 'text-slate-400 hover:text-slate-200 hover:bg-slate-700/50',
          )}
        >
          {t.label}
        </button>
      ))}
    </div>
  );
}

// ── Empty state ───────────────────────────────────────────────────────────────
export function EmptyState({ message, icon = '○' }: { message: string; icon?: string }) {
  return (
    <div className="flex flex-col items-center justify-center py-14 text-center gap-2">
      <span className="text-3xl opacity-20">{icon}</span>
      <p className="text-slate-500 text-sm">{message}</p>
    </div>
  );
}

// ── Status dot ────────────────────────────────────────────────────────────────
const DOT_CLR = {
  ok:      'bg-green-500',
  degraded:'bg-amber-500',
  down:    'bg-red-500',
  unknown: 'bg-slate-600',
};

export function StatusDot({ status }: { status: keyof typeof DOT_CLR }) {
  return (
    <span className="relative inline-flex h-2.5 w-2.5">
      <span className={cn('inline-flex rounded-full h-full w-full', DOT_CLR[status])} />
      {status === 'ok' && (
        <span className={cn('animate-ping absolute inline-flex h-full w-full rounded-full opacity-40', DOT_CLR[status])} />
      )}
    </span>
  );
}

// ── Page header ───────────────────────────────────────────────────────────────
export function PageHeader({
  title, subtitle, action,
}: { title: string; subtitle?: string; action?: ReactNode }) {
  return (
    <div className="flex items-start justify-between mb-6">
      <div>
        <h1 className="text-xl font-semibold text-slate-100">{title}</h1>
        {subtitle && <p className="text-sm text-slate-500 mt-0.5">{subtitle}</p>}
      </div>
      {action && <div className="flex-shrink-0">{action}</div>}
    </div>
  );
}

// ── Copy button ───────────────────────────────────────────────────────────────
export function CopyButton({ text, className }: { text: string; className?: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <button
      onClick={() => {
        navigator.clipboard.writeText(text).then(() => {
          setCopied(true);
          setTimeout(() => setCopied(false), 1500);
        });
      }}
      className={cn(
        'text-xs text-slate-500 hover:text-slate-300 transition-colors px-2 py-1 rounded border border-slate-700 hover:border-slate-600',
        className,
      )}
    >
      {copied ? '✓ Copied' : 'Copy'}
    </button>
  );
}

// ── Divider ───────────────────────────────────────────────────────────────────
export function Divider({ className }: { className?: string }) {
  return <hr className={cn('border-slate-800', className)} />;
}

// ── Score bar ─────────────────────────────────────────────────────────────────
export function ScoreBar({
  value, min = -1, max = 1, label,
}: { value: number; min?: number; max?: number; label?: string }) {
  const pct = Math.max(0, Math.min(100, ((value - min) / (max - min)) * 100));
  const color = value > 0.2 ? 'bg-green-500' : value < -0.2 ? 'bg-red-500' : 'bg-amber-500';
  return (
    <div className="w-full">
      {label && <p className="text-xs text-slate-500 mb-1">{label}</p>}
      <div className="w-full bg-slate-800 rounded-full h-1.5">
        <div className={cn('h-1.5 rounded-full transition-all', color)} style={{ width: `${pct}%` }} />
      </div>
      <p className="text-right text-xs text-slate-400 mt-0.5 tabular-nums">{value.toFixed(4)}</p>
    </div>
  );
}

// ── Inline code / mono ────────────────────────────────────────────────────────
export function Mono({ children, className }: { children: ReactNode; className?: string }) {
  return (
    <code className={cn('font-mono text-xs bg-slate-800 text-slate-300 px-1.5 py-0.5 rounded', className)}>
      {children}
    </code>
  );
}

// ── Section heading inside a card ─────────────────────────────────────────────
export function SectionTitle({ children }: { children: ReactNode }) {
  return <p className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-3">{children}</p>;
}
