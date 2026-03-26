import { authenticatedFetch } from './auth';

export interface StartInvestigationRequest {
  contextId: string;
  signalType: string;
  windowStart: string;
  windowEnd: string;
}

export interface StartInvestigationResponse {
  incidentId: string;
  status: 'RUNNING' | 'COMPLETED' | 'FAILED';
  duplicateRequest?: boolean;
}

async function parseApiError(response: Response): Promise<string> {
  const body = await response.json().catch(() => ({}));
  return body?.message ?? `Request failed (HTTP ${response.status}).`;
}

export async function startInvestigation(
  investigationsApiUrl: string,
  request: StartInvestigationRequest,
): Promise<StartInvestigationResponse> {
  const response = await authenticatedFetch(investigationsApiUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(request),
  });

  if (!response.ok) {
    throw new Error(await parseApiError(response));
  }

  return (await response.json()) as StartInvestigationResponse;
}
