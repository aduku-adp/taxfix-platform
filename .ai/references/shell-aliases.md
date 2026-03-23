# Shell Aliases for Claude Code

Two shell aliases streamline launching Claude Code sessions with this workspace.

## Setup

Add these lines to your `~/.zshrc` (or `~/.bashrc`):

```bash
alias cs='claude "/prime"'
alias cr='claude --dangerously-skip-permissions "/prime"'
