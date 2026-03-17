jest.mock('../src/utils/logger', () => ({
  debug: jest.fn(),
  error: jest.fn(),
  info: jest.fn(),
  warn: jest.fn(),
  api: jest.fn()
}))

const CodexCliValidator = require('../src/validators/clients/codexCliValidator')

describe('CodexCliValidator', () => {
  function createRequest(overrides = {}) {
    return {
      headers: {
        'user-agent': 'codex_cli_rs/0.38.0 (Ubuntu 22.4.0; x86_64) WindowsTerminal',
        originator: 'codex_cli_rs',
        session_id: '1234567890123456789012345'
      },
      path: '/openai/responses',
      body: {
        instructions:
          'You are Codex, based on GPT-5. You are running as a coding agent in the Codex CLI on a user\'s computer.',
        model: 'gpt-5-codex'
      },
      ...overrides
    }
  }

  it('accepts codex-tui user agent on non-strict paths', () => {
    const req = createRequest({
      headers: {
        'user-agent': 'codex-tui/0.115.0 (Ubuntu 22.4.0; x86_64) WarpTerminal (codex-tui; 0.115.0)'
      },
      path: '/v1/models'
    })

    expect(CodexCliValidator.validate(req)).toBe(true)
  })

  it('accepts codex-tui originator variants on strict paths', () => {
    const req = createRequest({
      headers: {
        'user-agent': 'codex-tui/0.115.0 (Ubuntu 22.4.0; x86_64) WarpTerminal (codex-tui; 0.115.0)',
        originator: 'codex_tui',
        session_id: '1234567890123456789012345'
      }
    })

    expect(CodexCliValidator.validate(req)).toBe(true)
  })

  it('rejects mismatched originator for codex-tui on strict paths', () => {
    const req = createRequest({
      headers: {
        'user-agent': 'codex-tui/0.115.0 (Ubuntu 22.4.0; x86_64) WarpTerminal (codex-tui; 0.115.0)',
        originator: 'codex_exec',
        session_id: '1234567890123456789012345'
      }
    })

    expect(CodexCliValidator.validate(req)).toBe(false)
  })
})
