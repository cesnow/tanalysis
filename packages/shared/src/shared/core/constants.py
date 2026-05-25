"""Application-wide constants shared across layers."""

# Jira issue types ingested by the sync flow.
# Extend this list to capture additional types.
JIRA_ISSUE_TYPES: list[str] = ["Backlog", "Epic", "Issue", "Task", "Sub-task"]
