import { cleanup, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, describe, expect, it } from 'vitest';
import IncidentContextDetails from './IncidentContextDetails';
import type { IncidentContext } from '../types/incidentContext';

afterEach(cleanup);

const BASE_CONTEXT: IncidentContext = {
  contextId: 'ctx_abc123',
  name: 'Production API',
  description: 'Core pipeline',
  region: 'eu-west-1',
  logGroups: ['/aws/lambda/order-svc', '/aws/lambda/payments'],
  metricDescriptors: [
    { namespace: 'AWS/Lambda', metricName: 'Errors', dimensions: { FunctionName: 'order-svc' }, stat: 'Average' },
  ],
  xrayServices: ['order-service'],
  createdAt: '2026-03-20T10:00:00Z',
  updatedAt: '2026-03-24T14:00:00Z',
  createdBy: 'user@example.com',
  updatedBy: 'user@example.com',
};

describe('IncidentContextDetails — metadata bar', () => {
  it('shows context id, region, and timestamp labels in the metadata bar', () => {
    render(<IncidentContextDetails context={BASE_CONTEXT} />);
    expect(screen.getByText('ctx_abc123')).toBeDefined();
    expect(screen.getByText('eu-west-1')).toBeDefined();
    expect(screen.getByText('Context ID')).toBeDefined();
    expect(screen.getByText('Region')).toBeDefined();
    expect(screen.getByText('Created')).toBeDefined();
    expect(screen.getByText('Updated')).toBeDefined();
  });
});

describe('IncidentContextDetails — resource tabs', () => {
  it('renders all three tabs even when xrayServices is empty', () => {
    render(<IncidentContextDetails context={{ ...BASE_CONTEXT, xrayServices: [] }} />);
    expect(screen.getByText('Log Groups')).toBeDefined();
    expect(screen.getByText('Metrics')).toBeDefined();
    expect(screen.getByText('X-Ray Services')).toBeDefined();
  });

  it('shows log group items in the Log Groups tab by default', () => {
    render(<IncidentContextDetails context={BASE_CONTEXT} />);
    expect(screen.getByText('/aws/lambda/order-svc')).toBeDefined();
  });

  it('switches to Metrics tab and shows metric rows', async () => {
    render(<IncidentContextDetails context={BASE_CONTEXT} />);
    await userEvent.click(screen.getByText('Metrics'));
    expect(screen.getByText(/AWS\/Lambda \/ Errors/)).toBeDefined();
  });

  it('shows empty state when active tab has no items', async () => {
    render(<IncidentContextDetails context={{ ...BASE_CONTEXT, xrayServices: [] }} />);
    await userEvent.click(screen.getByText('X-Ray Services'));
    expect(screen.getByText('None added.')).toBeDefined();
  });
});
