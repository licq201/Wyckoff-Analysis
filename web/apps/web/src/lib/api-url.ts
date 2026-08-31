const LOCAL_API_URL = 'http://127.0.0.1:8787'
const PRODUCTION_API_URL = 'https://wyckoff-api.yongkai-wang.workers.dev'

export function apiUrl(path: `/api/${string}`): string {
  const configured = import.meta.env.VITE_API_URL?.trim()
  if (configured) {
    return `${configured.replace(/\/$/, '')}${path}`
  }
  if (!import.meta.env.DEV) {
    return `${PRODUCTION_API_URL}${path}`
  }
  if (typeof window !== 'undefined' && window.location?.hostname && window.location.hostname !== 'localhost' && window.location.hostname !== '127.0.0.1') {
    return path
  }
  return `${LOCAL_API_URL}${path}`
}
