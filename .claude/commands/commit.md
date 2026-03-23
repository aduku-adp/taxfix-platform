# Commit

Generate a commit message for staged changes.

## Instructions

1. Run `git status` to confirm there are staged changes; if nothing is staged, say so and stop
2. Run `git diff --staged` to see what is staged
3. Write a commit message that explains WHY, not just WHAT

## Format
[<issue>](<type>) <subject>


<body>

### Types
- feat: New feature
- fix: Bug fix
- refactor: Code change (not fix or feature)
- test: Tests
- docs: Documentation
- chore: Maintenance


### Issue format
TFX-<YYYYMMDD>
Where YYYYMMDD is the current date


### Rules
- Subject: max 50 chars, imperative mood, start with upper case
- Body: explain WHY this change was needed
- Reference issues if applicable

## Example
[TFX-20260323](feat) Add password reset flow


Users were locked out permanently if they forgot their password.
This adds a reset flow using email verification tokens.

Closes #234

## Guidelines

- Future you will search git blame at 2am
- A good commit message is a gift to yourself
- If you can't summarize it, the change might be too big
