# Exception hierarchy for the streamq Python SDK.
#
# Why a hierarchy instead of a single StreamqError?
# Callers often want to handle specific error cases differently:
#
#   try:
#       await client.create_topic("payments")
#   except StreamqConflict:
#       pass  # already exists, that's fine
#   except StreamqError as e:
#       logger.error("broker error: %s", e)
#
# A flat exception with a status_code attribute would work, but requires
# callers to inspect attributes rather than using except clauses — less
# Pythonic and easy to forget.
#
# All SDK exceptions inherit from StreamqError so callers can catch
# everything with a single except if they don't need granularity.


class StreamqError(Exception):
    """Base class for all streamq SDK exceptions."""

    pass


class StreamqNotFound(StreamqError):
    """Raised when the broker returns 404.

    Typically: topic does not exist.
    """

    pass


class StreamqConflict(StreamqError):
    """Raised when the broker returns 409.

    Typically: topic already exists on create_topic().
    """

    pass


class StreamqAPIError(StreamqError):
    """Raised for any other non-2xx broker response.

    Attributes:
        status_code: The HTTP status code returned by the broker.
        message: The error message from the broker's JSON envelope.
    """

    def __init__(self, status_code: int, message: str) -> None:
        self.status_code = status_code
        self.message = message
        super().__init__(f"HTTP {status_code}: {message}")


class StreamqConsumerClosed(StreamqError):
    """Raised when iterating a consumer that was explicitly closed."""

    pass


class StreamqMaxReconnects(StreamqError):
    """Raised when a consumer exhausts its reconnect attempts.

    Attributes:
        attempts: Number of reconnect attempts made.
        last_error: The last connection error before giving up.
    """

    def __init__(self, attempts: int, last_error: Exception) -> None:
        self.attempts = attempts
        self.last_error = last_error
        super().__init__(f"gave up after {attempts} reconnect attempt(s): {last_error}")
