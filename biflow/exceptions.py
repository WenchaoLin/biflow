import warnings

class BiflowException(Exception):
    """
    Base class for all Biflow's errors.
    Each custom exception should be derived from this class.
    """

class BiflowNotFoundException(BiflowException):
    """Raise when the requested object/resource is not available in the system."""


class TaskNotFound(BiflowNotFoundException):
    """Raise when a Task is not available in the system."""

class DuplicateTaskIdFound(BiflowException):
    """Raise when a Task with duplicate task_id is defined in the same DAG."""