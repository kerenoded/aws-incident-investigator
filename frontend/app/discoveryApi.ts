import type { MetricDescriptor } from '../types/incidentContext';
import { authenticatedFetch } from './auth';

function discoveryBase(investigationsApiUrl: string): string {
  return investigationsApiUrl.replace(/\/investigations\/?$/, '/incident-contexts/discovery');
}

function buildQuery(params: Record<string, string | number | undefined | null>): string {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null) continue;
    const asString = String(value).trim();
    if (!asString) continue;
    query.set(key, asString);
  }
  const suffix = query.toString();
  return suffix ? `?${suffix}` : '';
}

async function parseApiError(response: Response): Promise<string> {
  const body = await response.json().catch(() => ({}));
  return body?.message ?? `Request failed (HTTP ${response.status}).`;
}

export interface PaginatedResponse<T> {
  items: T[];
  nextToken?: string | null;
}

export interface LogGroupDiscoveryItem {
  logGroupName: string;
  arn?: string;
}

export async function discoverLogGroups(
  investigationsApiUrl: string,
  params: { region: string; q?: string; pageSize?: number; nextToken?: string },
): Promise<PaginatedResponse<LogGroupDiscoveryItem>> {
  const query = buildQuery(params);
  const response = await authenticatedFetch(`${discoveryBase(investigationsApiUrl)}/log-groups${query}`);
  if (!response.ok) throw new Error(await parseApiError(response));
  return (await response.json()) as PaginatedResponse<LogGroupDiscoveryItem>;
}

export async function discoverMetrics(
  investigationsApiUrl: string,
  params: { region: string; namespace: string; q?: string; pageSize?: number; nextToken?: string },
): Promise<PaginatedResponse<MetricDescriptor>> {
  const query = buildQuery(params);
  const response = await authenticatedFetch(`${discoveryBase(investigationsApiUrl)}/metrics${query}`);
  if (!response.ok) throw new Error(await parseApiError(response));
  return (await response.json()) as PaginatedResponse<MetricDescriptor>;
}

export interface MetricNamespaceDiscoveryItem {
  namespace: string;
}

export async function discoverMetricNamespaces(
  investigationsApiUrl: string,
  params: { region: string; q: string; pageSize?: number },
): Promise<PaginatedResponse<MetricNamespaceDiscoveryItem>> {
  const query = buildQuery(params);
  const response = await authenticatedFetch(
    `${discoveryBase(investigationsApiUrl)}/metrics/namespaces${query}`,
  );
  if (!response.ok) throw new Error(await parseApiError(response));
  return (await response.json()) as PaginatedResponse<MetricNamespaceDiscoveryItem>;
}

export async function discoverXrayServices(
  investigationsApiUrl: string,
  params: { region: string; q?: string; lookbackMinutes?: number; pageSize?: number; nextToken?: string },
): Promise<PaginatedResponse<{ serviceName: string }>> {
  const query = buildQuery(params);
  const response = await authenticatedFetch(`${discoveryBase(investigationsApiUrl)}/xray/services${query}`);
  if (!response.ok) throw new Error(await parseApiError(response));
  return (await response.json()) as PaginatedResponse<{ serviceName: string }>;
}
