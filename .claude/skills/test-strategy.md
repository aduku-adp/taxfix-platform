---
name: test-strategy
description: Guidance on how to write and structure tests in this project
---

When writing or reviewing tests in this project:

- Prefer integration tests over mocks for data-layer logic
- Unit test pure business logic (calculations, validators) in isolation
- Test edge cases: zero values, boundary tax brackets, missing optional fields
- Use descriptive test names that read as specifications: "returns zero tax when income is below threshold"
- Avoid testing implementation details — test behavior and outcomes
