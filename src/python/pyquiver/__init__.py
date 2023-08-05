from ._quiver import (
    _clear_tracing,
    _info,
    _print_tracing,
    _print_tracing_histogram,
    _set_log_level,
)


def clear_tracing() -> None:
    """
    Clears all tracing data
    """
    _clear_tracing()


def print_tracing() -> None:
    """
    Prints tracing statistics to stdout

    The times will include all time spent since the last call to clear_tracing()
    """
    _print_tracing()


def print_tracing_histogram(width=40) -> None:
    """
    Alternative method of printing tracing statistics which prints tracing information
    as a visual histogram to stdout
    """
    _print_tracing_histogram(width)


def info() -> str:
    """
    Returns a string containing information about the current build
    """
    return _info()


def set_log_level(level: str) -> None:
    """
    Sets the log level for all operations going forwards.  Valid values are:

    trace - The most detailed level of logging, only available in debug builds
    debug - Detailed logging that may have slight impacts on performance
    info - Standard logging that should have no impact on performance
    """
    return _set_log_level(level)
