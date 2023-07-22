from ._quiver import _clear_tracing, _info, _print_tracing, _print_tracing_histogram


def clear_tracing() -> None:
    _clear_tracing()


def print_tracing() -> None:
    _print_tracing()


def print_tracing_histogram(width=40) -> None:
    _print_tracing_histogram(width)


def info() -> str:
    return _info()
