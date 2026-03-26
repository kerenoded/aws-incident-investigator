export interface MetricDescriptor {
  namespace?: string;
  metricName?: string;
  dimensions?: Record<string, string>;
  stat?: string;
  [key: string]: unknown;
}

export interface IncidentContext {
  contextId: string;
  name: string;
  description: string;
  region: string;
  logGroups: string[];
  metricDescriptors: MetricDescriptor[];
  xrayServices: string[];
  createdAt: string;
  updatedAt: string;
  createdBy: string;
  updatedBy: string;
}

export interface IncidentContextUpsertPayload {
  name: string;
  description?: string;
  region: string;
  logGroups?: string[];
  metricDescriptors?: MetricDescriptor[];
  xrayServices?: string[];
}
