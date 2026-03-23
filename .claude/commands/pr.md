Generate a pull request description for the current branch compared to main.

Structure:
## What
One sentence summary of the change.

## Why
The motivation or problem being solved.

## How
Key implementation decisions (skip if obvious).

## Testing
How this was tested or how reviewers can verify it.

Use `git log main..HEAD --oneline` and `git diff main` to understand the changes.
