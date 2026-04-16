#!/usr/bin/env python3
"""
A simple utility module with common functions.
"""

def greet(name):
    """Greet a person by name."""
    return f"Hello, {name}! Welcome to Python."


def add_numbers(a, b):
    """Add two numbers and return the result."""
    return a + b


def calculate_average(numbers):
    """Calculate the average of a list of numbers."""
    if not numbers:
        return 0
    return sum(numbers) / len(numbers)


def factorial(n):
    """Calculate factorial of a number."""
    if n < 0:
        raise ValueError("Factorial not defined for negative numbers")
    if n == 0 or n == 1:
        return 1
    return n * factorial(n - 1)


if __name__ == "__main__":
    # Test the functions
    print(greet("Sai"))
    print(f"5 + 3 = {add_numbers(5, 3)}")
    print(f"Average of [10, 20, 30] = {calculate_average([10, 20, 30])}")
    print(f"Factorial of 5 = {factorial(5)}")
