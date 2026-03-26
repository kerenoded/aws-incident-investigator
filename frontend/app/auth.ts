type TokenSet = {
  access_token: string;
  id_token?: string;
  refresh_token?: string;
  expires_at: number;
};

const COGNITO_REGION = import.meta.env.VITE_COGNITO_REGION as string | undefined;
const COGNITO_CLIENT_ID = import.meta.env.VITE_COGNITO_CLIENT_ID as string | undefined;
const COGNITO_DOMAIN = import.meta.env.VITE_COGNITO_DOMAIN as string | undefined;
const COGNITO_REDIRECT_URI =
  (import.meta.env.VITE_COGNITO_REDIRECT_URI as string | undefined) ?? window.location.origin;
const COGNITO_LOGOUT_URI =
  (import.meta.env.VITE_COGNITO_LOGOUT_URI as string | undefined) ?? window.location.origin;

const TOKENS_STORAGE_KEY = 'incident-investigator-auth-tokens';
const PKCE_VERIFIER_KEY = 'incident-investigator-pkce-verifier';
const OAUTH_STATE_KEY = 'incident-investigator-oauth-state';
const TOKEN_REFRESH_SKEW_SECONDS = 60;

// Singleton promise: if multiple callers observe an expired token at the same
// time, they all await the same refresh instead of racing to the token endpoint.
let _refreshPromise: Promise<string | null> | null = null;

// Singleton promise: guards against React StrictMode double-invoking the auth
// bootstrap effect, which would try to exchange the same one-time code twice.
let _bootstrapPromise: Promise<void> | null = null;

export class AuthRequiredError extends Error {
  constructor(message = 'Authentication required.') {
    super(message);
    this.name = 'AuthRequiredError';
  }
}

function assertAuthEnv(): void {
  if (!COGNITO_REGION || !COGNITO_CLIENT_ID || !COGNITO_DOMAIN) {
    throw new Error(
      'Missing Cognito auth env vars. Set VITE_COGNITO_REGION, VITE_COGNITO_CLIENT_ID, and VITE_COGNITO_DOMAIN.',
    );
  }
}

function tokenEndpoint(): string {
  assertAuthEnv();
  return `https://${COGNITO_DOMAIN}/oauth2/token`;
}

function authorizeEndpoint(): string {
  assertAuthEnv();
  return `https://${COGNITO_DOMAIN}/oauth2/authorize`;
}

function logoutEndpoint(): string {
  assertAuthEnv();
  return `https://${COGNITO_DOMAIN}/logout`;
}

function randomString(length = 64): string {
  const bytes = new Uint8Array(length);
  window.crypto.getRandomValues(bytes);
  return Array.from(bytes, (b) => (b % 36).toString(36)).join('');
}

function base64UrlEncode(buffer: ArrayBuffer): string {
  const bytes = new Uint8Array(buffer);
  let binary = '';
  bytes.forEach((b) => {
    binary += String.fromCharCode(b);
  });
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '');
}

async function createCodeChallenge(verifier: string): Promise<string> {
  const digest = await window.crypto.subtle.digest('SHA-256', new TextEncoder().encode(verifier));
  return base64UrlEncode(digest);
}

function saveTokens(tokens: TokenSet): void {
  localStorage.setItem(TOKENS_STORAGE_KEY, JSON.stringify(tokens));
}

function loadTokens(): TokenSet | null {
  const raw = localStorage.getItem(TOKENS_STORAGE_KEY);
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw) as TokenSet;
    if (!parsed.access_token || !parsed.expires_at) return null;
    return parsed;
  } catch {
    return null;
  }
}

function clearTokens(): void {
  localStorage.removeItem(TOKENS_STORAGE_KEY);
}

async function exchangeCodeForTokens(code: string, verifier: string): Promise<TokenSet> {
  assertAuthEnv();

  const body = new URLSearchParams({
    grant_type: 'authorization_code',
    client_id: COGNITO_CLIENT_ID!,
    code,
    code_verifier: verifier,
    redirect_uri: COGNITO_REDIRECT_URI,
  });

  const response = await fetch(tokenEndpoint(), {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body,
  });

  if (!response.ok) {
    throw new Error(`Token exchange failed (HTTP ${response.status}).`);
  }

  const data = await response.json() as {
    access_token: string;
    id_token?: string;
    refresh_token?: string;
    expires_in: number;
  };

  return {
    access_token: data.access_token,
    id_token: data.id_token,
    refresh_token: data.refresh_token,
    expires_at: Math.floor(Date.now() / 1000) + data.expires_in,
  };
}

async function refreshTokens(refreshToken: string): Promise<TokenSet> {
  assertAuthEnv();

  const body = new URLSearchParams({
    grant_type: 'refresh_token',
    client_id: COGNITO_CLIENT_ID!,
    refresh_token: refreshToken,
  });

  const response = await fetch(tokenEndpoint(), {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body,
  });

  if (!response.ok) {
    throw new Error(`Token refresh failed (HTTP ${response.status}).`);
  }

  const data = await response.json() as {
    access_token: string;
    id_token?: string;
    expires_in: number;
  };

  return {
    access_token: data.access_token,
    id_token: data.id_token,
    refresh_token: refreshToken,
    expires_at: Math.floor(Date.now() / 1000) + data.expires_in,
  };
}

function clearUrlAuthParams(): void {
  const url = new URL(window.location.href);
  url.searchParams.delete('code');
  url.searchParams.delete('state');
  url.searchParams.delete('error');
  url.searchParams.delete('error_description');
  window.history.replaceState({}, document.title, `${url.pathname}${url.search}${url.hash}`);
}

export async function signInWithHostedUi(): Promise<void> {
  assertAuthEnv();

  const verifier = randomString(96);
  const challenge = await createCodeChallenge(verifier);
  const state = randomString(32);

  sessionStorage.setItem(PKCE_VERIFIER_KEY, verifier);
  sessionStorage.setItem(OAUTH_STATE_KEY, state);

  const authUrl = new URL(authorizeEndpoint());
  authUrl.searchParams.set('response_type', 'code');
  authUrl.searchParams.set('client_id', COGNITO_CLIENT_ID!);
  authUrl.searchParams.set('redirect_uri', COGNITO_REDIRECT_URI);
  authUrl.searchParams.set('scope', 'openid email profile');
  authUrl.searchParams.set('code_challenge_method', 'S256');
  authUrl.searchParams.set('code_challenge', challenge);
  authUrl.searchParams.set('state', state);

  window.location.assign(authUrl.toString());
}

export function signOutFromHostedUi(): void {
  assertAuthEnv();
  clearTokens();
  sessionStorage.removeItem(PKCE_VERIFIER_KEY);
  sessionStorage.removeItem(OAUTH_STATE_KEY);

  const logoutUrl = new URL(logoutEndpoint());
  logoutUrl.searchParams.set('client_id', COGNITO_CLIENT_ID!);
  logoutUrl.searchParams.set('logout_uri', COGNITO_LOGOUT_URI);
  window.location.assign(logoutUrl.toString());
}

export async function bootstrapAuthSession(): Promise<void> {
  const url = new URL(window.location.href);
  const code = url.searchParams.get('code');
  const state = url.searchParams.get('state');
  const error = url.searchParams.get('error');

  if (error) {
    clearUrlAuthParams();
    throw new Error(`Sign-in failed: ${error}`);
  }

  // No auth code in URL — nothing to do.
  if (!code) return;

  // Coalesce concurrent bootstrap calls (e.g. React StrictMode double-invoke)
  // into a single token exchange so the one-time code is not used twice.
  if (_bootstrapPromise) return _bootstrapPromise;

  _bootstrapPromise = (async () => {
    try {
      const expectedState = sessionStorage.getItem(OAUTH_STATE_KEY);
      const verifier = sessionStorage.getItem(PKCE_VERIFIER_KEY);
      if (!expectedState || expectedState !== state || !verifier) {
        clearUrlAuthParams();
        throw new Error('Invalid OAuth callback state.');
      }

      const tokens = await exchangeCodeForTokens(code, verifier);
      saveTokens(tokens);
      sessionStorage.removeItem(PKCE_VERIFIER_KEY);
      sessionStorage.removeItem(OAUTH_STATE_KEY);
      clearUrlAuthParams();
    } finally {
      _bootstrapPromise = null;
    }
  })();

  return _bootstrapPromise;
}

export async function getValidAccessToken(): Promise<string | null> {
  const tokens = loadTokens();
  if (!tokens) return null;

  const now = Math.floor(Date.now() / 1000);
  if (tokens.expires_at > now + TOKEN_REFRESH_SKEW_SECONDS) {
    return tokens.access_token;
  }

  if (!tokens.refresh_token) {
    clearTokens();
    return null;
  }

  // Coalesce concurrent refresh calls into a single request.
  if (!_refreshPromise) {
    _refreshPromise = (async () => {
      try {
        const refreshed = await refreshTokens(tokens.refresh_token!);
        saveTokens(refreshed);
        return refreshed.access_token;
      } catch {
        clearTokens();
        return null;
      } finally {
        _refreshPromise = null;
      }
    })();
  }
  return _refreshPromise;
}

export async function isAuthenticated(): Promise<boolean> {
  const token = await getValidAccessToken();
  return Boolean(token);
}

export async function authenticatedFetch(input: string, init: RequestInit = {}): Promise<Response> {
  const accessToken = await getValidAccessToken();
  if (!accessToken) {
    throw new AuthRequiredError();
  }

  const headers = new Headers(init.headers ?? {});
  headers.set('Authorization', `Bearer ${accessToken}`);

  return fetch(input, {
    ...init,
    headers,
  });
}
