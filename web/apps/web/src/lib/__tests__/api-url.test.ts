import { afterEach, describe, expect, it, vi } from 'vitest'
import { apiUrl } from '../api-url'

afterEach(() => vi.unstubAllEnvs())

describe('apiUrl', () => {
  it('uses the configured backend without a duplicate slash', () => {
    vi.stubEnv('VITE_API_URL', 'https://api.example.com/')

    expect(apiUrl('/api/chat')).toBe('https://api.example.com/api/chat')
  })

  it('uses relative path when accessed via LAN IP host in development', () => {
    vi.stubEnv('VITE_API_URL', '')
    const originalWindow = globalThis.window
    try {
      globalThis.window = { location: { hostname: '192.168.3.99' } } as unknown as Window & typeof globalThis
      expect(apiUrl('/api/chat/config')).toBe('/api/chat/config')
    } finally {
      globalThis.window = originalWindow
    }
  })
})
