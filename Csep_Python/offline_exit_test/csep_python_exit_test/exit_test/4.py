"""
Validation Decorators for Input Data
"""

# imports
from functools import wraps

# Decorator 1: ensure_no_missing
def ensure_no_missing(func):
    @wraps(func)
    def wrapper(values):
        if values is None:
            raise ValueError("Input cannot be None")

        if not isinstance(values, list):
            raise TypeError("Input must be a list")

        for v in values:
            if v is None or v == "":
                raise ValueError("Missing values detected: None or empty string")

        return func(values)
    return wrapper

# Decorator 2: ensure_list_of_ints
def ensure_list_of_ints(func):
    @wraps(func)
    def wrapper(values):
        if not isinstance(values, list):
            raise TypeError("Input must be a list")

        for v in values:
            if not isinstance(v, int):
                raise TypeError(f"Invalid value: {v} is not an integer")

        return func(values)
    return wrapper

# Function with both decorators
@ensure_list_of_ints
@ensure_no_missing
def compute_statistics(values):
    return {
        "min": min(values),
        "max": max(values),
        "avg": sum(values) / len(values)
    }

# mainline execution
if __name__ == "__main__":
    print(compute_statistics([10, 20, 30, 40]))
    # compute_statistics([10, 20, None])
    # compute_statistics([10, "20", 30])
    # compute_statistics("not a list")