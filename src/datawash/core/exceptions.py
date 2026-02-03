"""Custom exceptions for datawash."""


class DataWashError(Exception):
    """Base exception for datawash."""


class AdapterError(DataWashError):
    """Error loading or saving data."""


class DetectionError(DataWashError):
    """Error during issue detection."""


class TransformationError(DataWashError):
    """Error applying a transformation."""


class ConfigError(DataWashError):
    """Error in configuration."""
