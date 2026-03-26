import type { IncidentContext, IncidentContextUpsertPayload } from '../types/incidentContext';
import { authenticatedFetch } from './auth';

function contextsApiBase(investigationsApiUrl: string): string {
  return investigationsApiUrl.replace(/\/investigations\/?$/, '/incident-contexts');
}

async function parseApiError(response: Response): Promise<string> {
  const body = await response.json().catch(() => ({}));
  return body?.message ?? `Request failed (HTTP ${response.status}).`;
}

export async function listIncidentContexts(investigationsApiUrl: string): Promise<IncidentContext[]> {
  const response = await authenticatedFetch(contextsApiBase(investigationsApiUrl));
  if (!response.ok) throw new Error(await parseApiError(response));
  const body = (await response.json()) as { items?: IncidentContext[] };
  return Array.isArray(body.items) ? body.items : [];
}

export async function createIncidentContext(
  investigationsApiUrl: string,
  payload: IncidentContextUpsertPayload,
): Promise<IncidentContext> {
  const response = await authenticatedFetch(contextsApiBase(investigationsApiUrl), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  if (!response.ok) throw new Error(await parseApiError(response));
  return (await response.json()) as IncidentContext;
}

export async function getIncidentContext(
  investigationsApiUrl: string,
  contextId: string,
): Promise<IncidentContext> {
  const response = await authenticatedFetch(`${contextsApiBase(investigationsApiUrl)}/${contextId}`);
  if (!response.ok) throw new Error(await parseApiError(response));
  return (await response.json()) as IncidentContext;
}

export async function updateIncidentContext(
  investigationsApiUrl: string,
  contextId: string,
  payload: Partial<IncidentContextUpsertPayload>,
): Promise<IncidentContext> {
  const response = await authenticatedFetch(`${contextsApiBase(investigationsApiUrl)}/${contextId}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  if (!response.ok) throw new Error(await parseApiError(response));
  return (await response.json()) as IncidentContext;
}

export async function deleteIncidentContext(
  investigationsApiUrl: string,
  contextId: string,
): Promise<void> {
  const response = await authenticatedFetch(`${contextsApiBase(investigationsApiUrl)}/${contextId}`, {
    method: 'DELETE',
  });
  if (!response.ok) throw new Error(await parseApiError(response));
}
