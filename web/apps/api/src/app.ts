import { Hono } from 'hono'
import { bodyLimit } from 'hono/body-limit'
import { cors } from 'hono/cors'
import { requestId } from 'hono/request-id'
import { secureHeaders } from 'hono/secure-headers'
import type { AgentRunMessage } from './services/agent-run'
import { logUnhandledWorkerError } from './services/request-observability'

export type Env = {
  SUPABASE_URL?: string
  SUPABASE_ANON_KEY?: string
  SUPABASE_SERVICE_ROLE_KEY?: string
  VITE_SUPABASE_URL?: string
  VITE_SUPABASE_ANON_KEY?: string
  TICKFLOW_API_BASE?: string
  PORTFOLIO_HKD_CNY_RATE?: string
  PORTFOLIO_USD_CNY_RATE?: string
  CHAT_DAILY_LIMIT_PER_USER?: string
  CHAT_MIN_INTERVAL_MS?: string
  CHAT_TOOL_APPROVAL_SECRET?: string
  UPSTASH_REDIS_REST_URL?: string
  UPSTASH_REDIS_REST_TOKEN?: string
  AGENT_SANDBOX_ENABLED?: string
  AGENT_SANDBOX_TIMEOUT_MS?: string
  AGENT_RUN_TTL_SECONDS?: string
  AGENT_RUN_DAILY_LIMIT_PER_USER?: string
  AGENT_RUN_DAILY_CPU_LIMIT_MS?: string
  AGENT_RUN_MIN_INTERVAL_MS?: string
  AGENT_RUN_QUEUE?: Queue<AgentRunMessage>
  AGENT_RUN_NOTIFIER?: DurableObjectNamespace
  REMOTE_RELAY?: DurableObjectNamespace
  SANDBOX_BRIDGE_URL?: string
  SANDBOX_BRIDGE_SECRET?: string
}

const ALLOWED_CORS_ORIGIN_PATTERNS = [
  /^http:\/\/localhost(:\d+)?$/,
  /^http:\/\/127\.0\.0\.1(:\d+)?$/,
  /^http:\/\/192\.168\.\d+\.\d+(:\d+)?$/,
  /^http:\/\/10\.\d+\.\d+\.\d+(:\d+)?$/,
  /^http:\/\/172\.(1[6-9]|2\d|3[0-1])\.\d+\.\d+(:\d+)?$/,
  /^https:\/\/[a-z0-9-]+\.pages\.dev$/,
]

export function isAllowedCorsOrigin(origin: string): boolean {
  if (!origin) return false
  return ALLOWED_CORS_ORIGIN_PATTERNS.some((pattern) => pattern.test(origin))
}

export type RuntimeReadinessCheck = (env: Env) => string[]

export function createApiApp(readinessCheck: RuntimeReadinessCheck = () => []) {
  const app = new Hono<{ Bindings: Env }>()

  app.use('*', requestId({ limitLength: 128 }))
  app.use('*', secureHeaders())
  app.use('*', cors({
    origin: (origin) => (isAllowedCorsOrigin(origin) ? origin : null),
    credentials: true,
  }))
  app.use('/api/*', bodyLimit({
    maxSize: 256 * 1024,
    onError: (c) => c.json({ error: 'Request body is too large', requestId: c.get('requestId') }, 413),
  }))

  app.onError((error, c) => {
    logUnhandledWorkerError(error, c)
    return c.json({ error: 'Internal Server Error', requestId: c.get('requestId') }, 500)
  })
  app.notFound((c) => c.json({ error: 'Not Found', requestId: c.get('requestId') }, 404))
  app.get('/api/health', (c) => {
    const missing = readinessCheck(c.env)
    return missing.length === 0
      ? c.json({ status: 'ok' })
      : c.json({ status: 'unhealthy', missing }, 503)
  })
  return app
}
