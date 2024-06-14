class PySparkExpandError(Exception):
    """Base class for PySparkExpand specific errors"""


class InvalidCardinalityError(PySparkExpandError):
    """Invalid cardinality value. Cardinality must be non-negative"""
