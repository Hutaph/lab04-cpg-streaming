"""Domain-level exceptions and errors."""


class DomainError(Exception):
    """Base domain exception."""


class ParsingError(DomainError):
    """Raised when an unrecoverable syntax or structural error occurs during parsing."""


class RepositoryNotFoundError(DomainError):
    """Raised when the target repository to analyze is not found on disk."""


class StateStoreError(DomainError):
    """Raised when there is an issue reading/writing the parsing state."""


class PublishError(DomainError):
    """Raised when writing or publishing events to external brokers fails."""


class SchemaValidationError(DomainError):
    """Raised when an event fails JSON schema validation."""


class EventSerializationError(DomainError):
    """Raised when an event fails to serialize to JSON bytes."""
