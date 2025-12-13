"""Functional Programming"""
# imports

from functools import reduce
from functools import partial, lru_cache, reduce
from itertools import groupby,permutations,combinations


# ------------- Case 1: Pure Functions and Immutability ------------- #

print("# ---------- Case 1: Pure Functions and Immutability ---------- #")

prices = [1200, 3000, 1500, 800, 2000]
discount_rate = 0.1

# 1. Pure function - apply discount
def apply_discount(price, rate):
    return price - (price * rate)

# 2. Pure function - filter expensive products
def filter_expensive(products, threshold):
    return list(filter(lambda p: p > threshold, products))

# Apply discount
discounted_prices = list(map(lambda p: apply_discount(p, discount_rate), prices))

# Filter expensive items
expensive_products = filter_expensive(prices, 1500)

# Show immutability
print("Original Prices:", prices)
print("Discounted Prices:", discounted_prices)
print("Expensive Products:", expensive_products)

# ------------- Case 2: Higher-Order Functions ------------- #

print("# ---------- Case 2: Higher-Order Functions ---------- #")

readings = [12.4, 13.1, 11.8, 10.5, 15.0, 13.9]

# 1. Higher-order function
def apply_to_readings(func, readings):
    return func(readings)

# 2. Use it with various functions
minimum = apply_to_readings(min, readings)
maximum = apply_to_readings(max, readings)
avg = apply_to_readings(lambda r: sum(r) / len(r), readings)

# 3. Print the results in a dictionary
results = {
    "min": minimum,
    "max": maximum,
    "avg": round(avg, 2)
}

print(results)

# ------------- Case 3: Recursion ------------- #

print("# ---------- Case 3: Recursion ---------- #")


company = {
    "Engineering": {
        "Backend": [80000, 90000],
        "Frontend": [75000, 70000]
    },
    "HR": [60000, 65000],
    "Finance": {
        "Accounts": [72000],
        "Audit": [68000, 70000]
    }
}

def total_salary(data):
    # If data is a list - base case - sum it
    if isinstance(data, list):
        return sum(data)

    # If data is a dict - recursive case
    total = 0
    for key in data:
        total += total_salary(data[key])   # recursive call
    return total

print("Total Salary:", total_salary(company))

# ------------- Case 4: Function Composition ------------- #

print("# ---------- Case 4: Function Composition ---------- #")

raw_data = [" 25.5 ", "26.8C", " 24.0 ", "error", "27.5c"]

# 1. Pure Functions

def strip_units(value):
    return value.replace("C", "").replace("c", "")

def strip_whitespace(value):
    return value.strip()

def to_float(value):
    try:
        return float(value)
    except:
        return None

# 2A. Composition (Nested Function Approach)

def clean_value_nested(v):
    return to_float(strip_units(strip_whitespace(v)))

# 2B. Composition (Functional Pipeline using reduce)

pipeline = [strip_whitespace, strip_units, to_float]

def clean_value_reduce(v):
    return reduce(lambda acc, fn: fn(acc), pipeline, v)

# 3. Apply both pipelines + filter invalid

cleaned_nested = [clean_value_nested(v) for v in raw_data]
valid_nested = list(filter(lambda x: x is not None, cleaned_nested))

cleaned_reduce = [clean_value_reduce(v) for v in raw_data]
valid_reduce = list(filter(lambda x: x is not None, cleaned_reduce))

print("Nested Composition Output:", cleaned_nested)
print("Valid Readings (Nested):", valid_nested)

print("\nReduce Composition Output:", cleaned_reduce)
print("Valid Readings (Reduce):", valid_reduce)

# ------------- Case 5: Iterators and Generators ------------- #

print("# ---------- Case 5: Iterators and Generators ---------- #")
def sensor_data():
    readings = [22.4, 22.8, 23.1, 24.0, 25.2, 24.9, 26.0]

    for r in readings:
        print(f"Producing reading: {r}")   # Lazy production
        yield r


# Main simulation
gen = sensor_data()

print("\n--- Consuming first 3 readings ---")
for _ in range(3):
    print("Received:", next(gen))

print("\n--- Remaining readings have NOT been produced yet ---")
# The generator pauses here; remaining readings are untouched.

# ------------- Case 6: Functools ------------- #

print("# ---------- Case 6: Functools ---------- #")

# 1. Partial Function for Scaling
# scale_value(x) = (x - 50) / 10
scale_value = partial(lambda x, m, s: (x - m) / s, m=50, s=10)

# Input values
values = [45, 50, 60]

# 2. Apply scaling
scaled_values = list(map(scale_value, values))
print("Scaled Values:", scaled_values)

# 3. Memoized Fibonacci using @lru_cache
@lru_cache(maxsize=None)
def fibonacci(n):
    if n <= 1:
        return n
    return fibonacci(n - 1) + fibonacci(n - 2)

print("Fibonacci(10):", fibonacci(10))

# 4. Use reduce() to compute product of scaled values
product_scaled = reduce(lambda a, b: a * b, scaled_values)
print("Product of Scaled Values:", product_scaled)

# ------------- Case 7: Itertools ------------- #

print("# ---------- Case 7: Itertools ---------- #")

features = ['age', 'income', 'education', 'credit_score']

# 1. Feature pairs and triplets
feature_pairs = list(combinations(features, 2))
feature_triplets = list(combinations(features, 3))

print("Feature Pairs:", feature_pairs)
print("Feature Triplets:", feature_triplets)

# 2. Feature permutations
feature_orders = list(permutations(features))
print("Total possible orderings:", len(feature_orders))

# 3. Grouping transaction categories
categories = [
    "food", "utilities", "food", "travel", "travel",
    "shopping", "food", "utilities"
]

categories_sorted = sorted(categories)

grouped = {
    key: list(group)
    for key, group in groupby(categories_sorted)
}

print("Grouped Categories:", grouped)

total_models = len(feature_pairs) + len(feature_triplets)
print("Total Models to Train:", total_models)

# ------------- Case 8: Real-World Mini Project ------------- #

print("# ---------- Case 8: Real-World Mini Project ---------- #")

data = [
    ("S1", "25.5C", "2025-10-26 10:00"),
    ("S2", "27.0",  "2025-10-26 10:05"),
    ("S1", "invalid", "2025-10-26 10:10"),
    ("S3", "26.5c", "2025-10-26 10:15"),
]

# 1. PURE FUNCTIONS for cleaning
def strip_units(v):
    return v.replace("C", "").replace("c", "")

def to_float(v):
    try:
        return float(v)
    except:
        return None


# 2. Generator pipeline (memory efficient)
def clean_data(records):
    for sensor, temp, ts in records:
        cleaned = to_float(strip_units(temp))
        yield (sensor, cleaned, ts)


# apply map/filter using generator
# Step 1: clean temperatures
cleaned_gen = clean_data(data)

# Step 2: filter out invalid readings
valid_gen = filter(lambda r: r[1] is not None, cleaned_gen)

# (groupby requires sorting first)
valid_list = list(valid_gen)  # grouping requires one pass
valid_list.sort(key=lambda x: x[0])  # sort by sensor id

groups = groupby(valid_list, key=lambda x: x[0])


# 3. Reduce - calculate average per sensor
sensor_avg = {}

for sensor, readings in groups:
    temps = [r[1] for r in readings]
    avg = reduce(lambda a, b: a + b, temps) / len(temps)
    sensor_avg[sensor] = round(avg, 2)

print("Clean + Valid Data:", valid_list)
print("Average Temperature Per Sensor:", sensor_avg)