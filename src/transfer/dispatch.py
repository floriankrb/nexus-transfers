"""Dispatch table – functions exposed by every client."""


def adder(value):
    """Add 1 to a numeric value.

    Parameters
    ----------
    value
        A number to increment.
    """
    return value + 1


def echo(*args):
    """Return the arguments unchanged.

    Parameters
    ----------
    *args
        Arbitrary positional arguments.
    """
    if len(args) == 1:
        return args[0]
    return list(args)


DISPATCH = {
    "adder": adder,
    "echo": echo,
}
