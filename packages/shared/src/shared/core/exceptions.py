"""Application-wide custom exceptions."""


class TAnalysisError(Exception):
    """Base exception for all TAnalysis domain errors."""


class ProductNotFoundError(TAnalysisError):
    """Raised when a requested product does not exist."""


class JiraAPIError(TAnalysisError):
    """Raised when the Jira REST API returns an unexpected error."""


class MongoDBError(TAnalysisError):
    """Raised when a MongoDB operation fails."""


class DataPipelineError(TAnalysisError):
    """Raised when an ETL flow encounters an unrecoverable error."""
