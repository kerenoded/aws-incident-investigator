/**
 * Smoke tests for App.tsx.
 *
 * These tests verify early-return rendering paths without spinning up
 * auth or API dependencies. The auth and API modules are fully mocked
 * so tests are hermetic and fast.
 */

import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import App from './App';

// Stub the auth module — tests do not need real Cognito interactions.
vi.mock('./auth', () => ({
  bootstrapAuthSession: vi.fn().mockReturnValue(new Promise(() => {})), // never resolves
  isAuthenticated: vi.fn().mockResolvedValue(false),
  signInWithHostedUi: vi.fn().mockResolvedValue(undefined),
  signOutFromHostedUi: vi.fn(),
}));

// Stub the contexts API so no real network calls are made.
vi.mock('./incidentContextsApi', () => ({
  listIncidentContexts: vi.fn().mockResolvedValue([]),
  getIncidentContext: vi.fn().mockResolvedValue(null),
  createIncidentContext: vi.fn().mockResolvedValue(null),
  updateIncidentContext: vi.fn().mockResolvedValue(null),
  deleteIncidentContext: vi.fn().mockResolvedValue(undefined),
}));

afterEach(cleanup);

describe('App — auth loading state', () => {
  it('renders the app shell while auth is bootstrapping', () => {
    // Auth is permanently pending (bootstrapAuthSession never resolves),
    // so the component should show its loading state.
    render(<App />);
    expect(screen.getByRole('heading', { name: /incident investigator/i })).toBeDefined();
    expect(screen.getByText(/loading authentication/i)).toBeDefined();
  });
});
