## Guidelines for Contributions

Thank you for considering contributing to this project! We welcome contributions of all kinds, including bug fixes, new features, documentation improvements, and more. To help ensure a smooth contribution process, please follow these guidelines:

- Keep changes focused and scoped to a specific improvement or fix.
- Prefer small, reviewable pull requests over large multi-purpose changes.
- Match existing project structure, naming, and style whenever possible.
- Avoid unrelated refactors in feature or bug-fix pull requests.

## AI Code Contributions

AI-assisted contributions are welcome. Contributors are expected to fully understand any AI-generated code they submit and be able to reason the proposed implementation.

## Coding Standards- Follow PEP8 for Python formatting.

- Use clear, descriptive variable and function names.
- Add comments only when they improve clarity. Avoid redundant comments.
- Aim for code that reads naturally and is easy to follow.
- Keep functions small and focused on a single responsibility.
- Reuse existing utilities and patterns before introducing new abstractions.
- Public functions intended for library users should be exported in the appropriate `__init__.py`.
- Private functions and internal helpers should be prefixed with an underscore, for example `_my_function`.

## Testing

- Test changes locally before submitting.
- Add tests for behavior changes and bug fixes when appropriate.
- Avoid redundant or excessive tests that add maintenance burden without improving confidence.
- Prefer precise, meaningful tests that verify observable behavior.

## Documentation

- Update documentation for any change in behavior, public API, configuration, or developer workflow.
- Keep documentation consistent with the current codebase and existing style.
- Use concise examples when they make usage clearer.

## Pull Request Expectations

- Write clear pull request descriptions that explain the problem, the change, and any tradeoffs.
- Reference related issues when applicable.
- Keep commits and pull requests easy to review.
- Ensure tests and checks pass before requesting review.

## General Best Practices

- Prefer readability over cleverness.
- Handle errors explicitly where appropriate.
- Avoid introducing unnecessary dependencies.
- Preserve backward compatibility unless a breaking change is absolutely necessary.
