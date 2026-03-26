import { useEffect, useRef, useState } from 'react';
import type { TriggerFormValues } from '../components/TriggerForm';
import type { FinalReport, InvestigationStatus } from '../types/report';
import { AuthRequiredError, authenticatedFetch } from './auth';

export type InvestigationState =
  | { phase: 'idle' }
  | { phase: 'submitting' }
  | { phase: 'polling'; incidentId: string }
  | { phase: 'completed'; incidentId: string; report: FinalReport }
  | { phase: 'authExpired' }
  | { phase: 'unauthorized'; message: string }
  | { phase: 'error'; message: string };

const POLL_INTERVAL_MS = 3000;
// 600 polls × 3 s = 30-minute hard timeout to prevent infinite polling
// on a hung or stalled backend execution.
const MAX_POLL_ATTEMPTS = 600;

export function useInvestigationPoller(apiUrl: string) {
  const [state, setState] = useState<InvestigationState>({ phase: 'idle' });
  const [lastTriggerValues, setLastTriggerValues] = useState<TriggerFormValues | null>(null);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const pollAttemptsRef = useRef(0);

  useEffect(() => {
    return () => {
      if (pollRef.current !== null) clearInterval(pollRef.current);
    };
  }, []);

  function stopPolling() {
    if (pollRef.current !== null) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
  }

  function startPolling(incidentId: string) {
    stopPolling();
    pollAttemptsRef.current = 0;
    pollRef.current = setInterval(async () => {
      pollAttemptsRef.current += 1;
      if (pollAttemptsRef.current > MAX_POLL_ATTEMPTS) {
        stopPolling();
        setState({ phase: 'error', message: 'Investigation timed out after 30 minutes.' });
        return;
      }
      try {
        const statusRes = await authenticatedFetch(`${apiUrl}/${incidentId}`);
        if (statusRes.status === 401) {
          stopPolling();
          setState({ phase: 'authExpired' });
          return;
        }
        if (statusRes.status === 403) {
          stopPolling();
          setState({
            phase: 'unauthorized',
            message:
              'You are not authorized to access this investigation. Verify that your account has the correct service group assigned in Cognito.',
          });
          return;
        }
        if (!statusRes.ok) {
          stopPolling();
          setState({ phase: 'error', message: `Status check failed (HTTP ${statusRes.status}).` });
          return;
        }
        const status: InvestigationStatus = await statusRes.json();

        if (status.status === 'FAILED') {
          stopPolling();
          setState({ phase: 'error', message: 'Investigation failed on the server.' });
          return;
        }

        if (status.status === 'COMPLETED') {
          stopPolling();
          const reportRes = await authenticatedFetch(`${apiUrl}/${incidentId}/report`);
          if (reportRes.status === 401) {
            setState({ phase: 'authExpired' });
            return;
          }
          if (reportRes.status === 403) {
            setState({
              phase: 'unauthorized',
              message: 'You are not authorized to read this investigation report.',
            });
            return;
          }
          if (!reportRes.ok) {
            setState({ phase: 'error', message: `Failed to fetch report (HTTP ${reportRes.status}).` });
            return;
          }
          const report: FinalReport = await reportRes.json();
          setState({ phase: 'completed', incidentId, report });
        }
        // If RUNNING, keep polling.
      } catch (err) {
        if (err instanceof AuthRequiredError) {
          stopPolling();
          setState({ phase: 'authExpired' });
          return;
        }
        stopPolling();
        setState({ phase: 'error', message: `Network error: ${String(err)}` });
      }
    }, POLL_INTERVAL_MS);
  }

  function handleStarted(incidentId: string, submittedValues: TriggerFormValues) {
    setLastTriggerValues(submittedValues);
    setState({ phase: 'submitting' });
    setState({ phase: 'polling', incidentId });
    startPolling(incidentId);
  }

  function handleReset() {
    stopPolling();
    setState({ phase: 'idle' });
  }

  return {
    state,
    lastTriggerValues,
    onStarted: handleStarted,
    onReset: handleReset,
  };
}
