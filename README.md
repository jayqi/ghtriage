# ghtriage

**GitHub project management and triage tool.**

This package provides a command-line interface (CLI) for:

- pulling all issue, pull request, and comment data for a GitHub repository into a local DuckDB database
- inspecting the local database schema
- querying the local database

The motivation is to provide a local snapshot of the GitHub data that an AI coding agent can cheaply query to help perform project management and triage tasks, such as identifying stale issues by their content. This data would be complemented by the actual commit history available from the local Git repository.
