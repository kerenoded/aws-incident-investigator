import type { CSSProperties } from 'react';

export type ButtonVariant = 'primary' | 'secondary' | 'ghost' | 'danger';

function pad2(value: number): string {
  return String(value).padStart(2, '0');
}

export function formatDateTime(value: string | undefined | null): string {
  if (!value) return '';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  const day = pad2(date.getDate());
  const month = pad2(date.getMonth() + 1);
  const year = date.getFullYear();
  const hours = pad2(date.getHours());
  const minutes = pad2(date.getMinutes());
  const seconds = pad2(date.getSeconds());
  return `${day}/${month}/${year} ${hours}:${minutes}:${seconds}`;
}

export function formatDateTimeUtc(value: string | undefined | null): string {
  if (!value) return '';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  const day = pad2(date.getUTCDate());
  const month = pad2(date.getUTCMonth() + 1);
  const year = date.getUTCFullYear();
  const hours = pad2(date.getUTCHours());
  const minutes = pad2(date.getUTCMinutes());
  const seconds = pad2(date.getUTCSeconds());
  return `${day}/${month}/${year} ${hours}:${minutes}:${seconds}`;
}

export function buttonStyle(variant: ButtonVariant = 'secondary', disabled = false): CSSProperties {
  const base: CSSProperties = {
    borderRadius: 8,
    border: '1px solid transparent',
    padding: '8px 14px',
    fontSize: '0.875rem',
    fontWeight: 500,
    cursor: disabled ? 'not-allowed' : 'pointer',
    transition: 'all 140ms ease',
    opacity: disabled ? 0.55 : 1,
    lineHeight: 1.4,
  };

  const variants: Record<ButtonVariant, CSSProperties> = {
    primary: {
      background: '#2563eb',
      color: '#fff',
      borderColor: '#2563eb',
      boxShadow: '0 1px 2px rgba(37,99,235,0.2)',
    },
    secondary: {
      background: '#f8fafc',
      color: '#1e40af',
      borderColor: '#cbd5e1',
    },
    ghost: {
      background: 'transparent',
      color: '#475569',
      borderColor: '#e2e8f0',
    },
    danger: {
      background: '#fef2f2',
      color: '#b91c1c',
      borderColor: '#fecaca',
    },
  };

  return { ...base, ...variants[variant] };
}

export const inputStyle: CSSProperties = {
  width: '100%',
  padding: '8px 10px',
  boxSizing: 'border-box',
  borderRadius: 8,
  border: '1px solid #d0d5dd',
  fontSize: '0.875rem',
  lineHeight: 1.5,
  color: '#1a2438',
  background: '#fff',
  transition: 'border-color 140ms ease',
  outline: 'none',
};

export const labelStyle: CSSProperties = {
  display: 'block',
  marginBottom: 10,
  fontSize: '0.85rem',
  fontWeight: 500,
  color: '#344054',
};

export const cardStyle: CSSProperties = {
  border: '1px solid #e2e8f0',
  borderRadius: 12,
  background: '#fff',
  boxShadow: '0 1px 3px rgba(15, 23, 42, 0.04)',
};

export type ResourceType = 'log' | 'metric' | 'xray';

interface ResourceTheme {
  icon: string;
  label: string;
  color: string;
  bg: string;
  border: string;
  accent: string;
  badgeBg: string;
  badgeColor: string;
}

export function resourceTypeTheme(type: ResourceType): ResourceTheme {
  switch (type) {
    case 'log':
      return { icon: '📋', label: 'Log Groups',     color: '#1e40af', bg: '#f8faff', border: '#dbeafe', accent: '#3b82f6', badgeBg: '#dbeafe', badgeColor: '#1d4ed8' };
    case 'metric':
      return { icon: '📊', label: 'Metrics',        color: '#15803d', bg: '#f0fdf4', border: '#bbf7d0', accent: '#22c55e', badgeBg: '#dcfce7', badgeColor: '#15803d' };
    case 'xray':
      return { icon: '🔍', label: 'X-Ray Services', color: '#6b21a8', bg: '#faf5ff', border: '#e9d5ff', accent: '#a855f7', badgeBg: '#f3e8ff', badgeColor: '#7c3aed' };
  }
}

export function resourceItemStyle(type: ResourceType): CSSProperties {
  const { bg, border, accent } = resourceTypeTheme(type);
  return {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    background: bg,
    border: `1px solid ${border}`,
    borderLeft: `3px solid ${accent}`,
    borderRadius: 6,
    padding: '7px 10px 7px 12px',
    fontFamily: 'monospace',
    fontSize: 12,
  };
}

export const metadataBarStyle: CSSProperties = {
  background: 'linear-gradient(to right, #f8fafc, #f0f4ff)',
  border: '1px solid #e2e8f0',
  borderRadius: 9,
  boxShadow: '0 1px 3px rgba(0,0,0,.06)',
};

export function tabStyle(active: boolean): CSSProperties {
  return {
    display: 'flex',
    alignItems: 'center',
    gap: 5,
    padding: '9px 14px',
    fontSize: 12,
    fontWeight: active ? 600 : 500,
    color: active ? '#2563eb' : '#64748b',
    background: active ? '#fff' : '#f8fafc',
    border: 'none',
    borderBottom: active ? '2px solid #2563eb' : '2px solid transparent',
    cursor: 'pointer',
    outline: 'none',
  };
}

export const tabStripStyle: CSSProperties = {
  display: 'flex',
  borderBottom: '1px solid #e2e8f0',
  background: '#f8fafc',
};
