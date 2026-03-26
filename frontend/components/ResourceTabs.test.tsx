import { cleanup, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, describe, expect, it, vi } from 'vitest';
import ResourceTabs from './ResourceTabs';

afterEach(cleanup);

describe('ResourceTabs', () => {
  const counts = { log: 3, metric: 1, xray: 0 };

  it('renders all three tabs regardless of zero counts', () => {
    render(<ResourceTabs activeTab="log" counts={counts} onChange={vi.fn()} />);
    expect(screen.getByText('Log Groups')).toBeDefined();
    expect(screen.getByText('Metrics')).toBeDefined();
    expect(screen.getByText('X-Ray Services')).toBeDefined();
  });

  it('calls onChange with the clicked tab type', async () => {
    const onChange = vi.fn();
    render(<ResourceTabs activeTab="log" counts={counts} onChange={onChange} />);
    await userEvent.click(screen.getByText('Metrics'));
    expect(onChange).toHaveBeenCalledWith('metric');
  });

  it('shows count badges for all tabs', () => {
    render(<ResourceTabs activeTab="log" counts={counts} onChange={vi.fn()} />);
    expect(screen.getByText('3')).toBeDefined(); // log badge
    expect(screen.getByText('1')).toBeDefined(); // metric badge
    expect(screen.getByText('0')).toBeDefined(); // xray badge (zero, still shown)
  });
});
