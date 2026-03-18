jest.mock('../src/utils/logger', () => ({
  debug: jest.fn(),
  error: jest.fn(),
  info: jest.fn(),
  warn: jest.fn(),
  api: jest.fn()
}))

const ClaudeCodeValidator = require('../src/validators/clients/claudeCodeValidator')
const ClientValidator = require('../src/validators/clientValidator')

describe('ClaudeCodeValidator', () => {
  function createRequest(overrides = {}) {
    return {
      headers: {
        'user-agent': 'claude-cli/2.1.78 (external, cli)'
      },
      path: '/api/v1/messages',
      body: {},
      ...overrides
    }
  }

  it('allows Claude Code user agent in default mode for client restrictions', () => {
    const req = createRequest()

    expect(ClaudeCodeValidator.validate(req)).toBe(true)
  })

  it('rejects strict Claude Code validation when required headers are missing', () => {
    const req = createRequest()

    expect(ClaudeCodeValidator.validate(req, { strict: true })).toBe(false)
  })

  it('accepts strict Claude Code validation without anthropic-beta when other required fields exist', () => {
    const req = createRequest({
      headers: {
        'user-agent': 'claude-cli/2.1.78 (external, cli)',
        'x-app': 'cli',
        'anthropic-version': '2023-06-01'
      },
      body: {
        metadata: {
          user_id:
            'user_d98385411c93cd074b2cefd5c9831fe77f24a53e4ecdcd1f830bba586fe62cb9_account__session_17cf0fd3-d51b-4b59-977d-b899dafb3022'
        }
      }
    })

    expect(ClaudeCodeValidator.validate(req, { strict: true })).toBe(true)
  })
})

describe('ClientValidator', () => {
  it('matches claude-cli user agent when claude_code is allowed', () => {
    const req = {
      headers: {
        'user-agent': 'claude-cli/2.1.78 (external, cli)'
      },
      path: '/api/v1/messages',
      body: {},
      ip: '127.0.0.1'
    }

    expect(ClientValidator.validateRequest(['claude_code', 'codex_cli'], req)).toMatchObject({
      allowed: true,
      matchedClient: 'claude_code',
      clientName: 'Claude Code'
    })
  })
})
