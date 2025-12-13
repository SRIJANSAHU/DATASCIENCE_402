"""Data strucures: List, Sets, Dictionary, Tuples"""

# imports
from pathlib import Path

# ------------- Contact Manager  -------------- #
contacts = [] # main contact list

def add_contact(name, phone, email):
    """Add a Contact"""
    contact = {
        "name": name,
        "phone": phone,
        "email": email
    }
    contacts.append(contact)
    print(f"Contact '{name}' added.")

def search_contact(keyword):
    """Search Contacts"""
    keyword = keyword.lower()
    results = []

    for contact in contacts:
        if keyword in contact["name"].lower():
            results.append(contact)

    return results

def delete_contact(name):
    """Delete a Contact"""
    name = name.lower()
    for contact in contacts:
        if contact["name"].lower() == name:
            contacts.remove(contact)
            print(f"✔ Contact '{name}' deleted.")
            return
    print("Contact not found.")

def list_contacts():
    """List Contacts Sorted"""
    sorted_list = sorted(contacts, key=lambda c: c["name"])
    return sorted_list

# ------------- Menu-Based Todo List  -------------- #

todo_list = [
    {"task": "Buy milk", "status": "pending"},
    {"task": "Pay bills", "status": "done"}
]

def add_task():
    task = input("Enter new task: ")
    todo_list.append({"task": task, "status": "pending"})
    print("Task added.")

def mark_done():
    task = input("Enter task to mark as done: ").lower()

    for item in todo_list:
        if item["task"].lower() == task:
            item["status"] = "done"
            print("Marked as done.")
            return

    print("Task not found.")

def delete_task():
    task = input("Enter task to delete: ").lower()

    for item in todo_list:
        if item["task"].lower() == task:
            todo_list.remove(item)
            print("Task deleted.")
            return

    print("Task not found.")

def show_pending():
    print("\nPending Tasks:")
    for item in todo_list:
        if item["status"] == "pending":
            print("-", item["task"])
    print()

# ------------- EvenIterator Class -------------- #

class EvenIterator:
    def __init__(self, start, end):
        self.start = start
        self.end = end
        self.current = start

        # Move to the first even number
        if self.current % 2 != 0:
            self.current += 1

    def __iter__(self):
        return self

    def __next__(self):
        if self.current > self.end:
            raise StopIteration

        value = self.current
        self.current += 2   # jump to next even number
        return value

# ------------- Generator for Fibonacci Series -------------- #

def fibonacci(n):
    """Generator for First n Fibonacci Numbers"""
    a, b = 0, 1
    for _ in range(n):
        yield a
        a, b = b, a + b

def fibonacci_infinite():
    """Infinite Fibonacci Generator"""
    a, b = 0, 1
    while True:
        yield a
        a, b = b, a + b

# ------------- Lazy Log File Reader -------------- #

def read_logs(file_path):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                yield line.rstrip("\n")
    except FileNotFoundError:
        print("Log file not found.")
    except PermissionError:
        print("Permission denied while reading the file.")

# ------------- CSV to Dictionary Converter -------------- #

def csv_to_dict_list(file_path):
    try:
        with open(file_path, "r",encoding="utf-8") as f:
            lines = [line.strip().split(",") for line in f]

        # First row is header
        header = lines[0]

        # Remaining rows = data
        data_rows = lines[1:]

        # Convert each row into a dictionary using zip()
        result = [
            {key: (int(value) if value.isdigit() else value) for key, value in zip(header, row)}
            for row in data_rows
        ]

        return result

    except FileNotFoundError:
        print("CSV file not found.")
    except PermissionError:
        print("Cannot access CSV file.")


# mainline execution
print("# ------------- CONTACT MANAGER -------------- #")

add_contact("Alice", "9876543210", "alice@example.com")
add_contact("Bob", "9991112233", "bob@example.com")
add_contact("Charlie", "8881234567", "charlie@abc.com")

print("\nSearch for 'bo':")
print(search_contact("bo"))

print("\nList Sorted:")
print(list_contacts())

print("\nDelete Bob:")
delete_contact("Bob")

print("\nFinal List:")
print(list_contacts())


sales = [
    ("2025-01-01", "TV", 45000),
    ("2025-01-01", "Mobile", 15000),
    ("2025-01-02", "TV", 47000),
    ("2025-01-02", "Laptop", 55000)
]

print("# ------------- Analyze Sales Data -------------- #")

# 1. Unique product names
unique_products = {product for (_, product, _) in sales}
print("Unique Products:", unique_products)

# 2. Dates with sales > 50,000
high_sale_dates = [date for (date, _, amount) in sales if amount > 50000]
print("High Sale Dates:", high_sale_dates)

# 3. Total sales per product
total_per_product = {}
for (_, product, amount) in sales:
    total_per_product[product] = total_per_product.get(product, 0) + amount

print("Total Sales per Product:", total_per_product)


print("# ------------- Tag Recommendation System -------------- #")

tags_user1 = {"python", "machine-learning", "ai"}
tags_user2 = {"python", "deep-learning", "ai", "math"}

# 1. Common interests
common_interests = tags_user1 & tags_user2
print("Common Interests:", common_interests)

# 2. Tags user1 should explore
suggest_to_user1 = tags_user2 - tags_user1
print("Suggestions for User1:", suggest_to_user1)

# 3. Combined interests
combined_interests = tags_user1 | tags_user2
print("Combined Interests:", combined_interests)


print("# ------------- Word Frequency Counter -------------- #")

paragraph = input("Enter a paragraph: ")
# Hello world! This is a test. Hello again, world.

# 1. Convert to lowercase
text = paragraph.lower()

# 2. Remove punctuations
punctuations = ",.!?;:\"'()-"
clean_text = "".join(ch for ch in text if ch not in punctuations)

# 3. Split into words
words = clean_text.split()

# 4. Count frequencies
freq = {}
for word in words:
    freq[word] = freq.get(word, 0) + 1

# 5. Keep only words occurring >1
more_than_once = {word: count for word, count in freq.items() if count > 1}

print("Word counts:", freq)
print("Words appearing more than once:", more_than_once)


print("# ------------- Menu-Based Todo List -------------- #")

'''
while True:
    print("\n--- TODO MENU ---")
    print("1. Add Task")
    print("2. Mark Task as Done")
    print("3. Delete Task")
    print("4. Show Pending Tasks")
    print("5. Exit")

    choice = input("Enter choice: ")

    if choice == "1":
        add_task()
    elif choice == "2":
        mark_done()
    elif choice == "3":
        delete_task()
    elif choice == "4":
        show_pending()
    elif choice == "5":
        print("Exiting...")
        break
    else:
        print("Invalid choice. Try again.")
'''

print("# ------------- EvenIterator Class -------------- #")

iterator = EvenIterator(3, 15)

for num in iterator:
    print(num)

print("# ------------- Fibonacci Generator -------------- #")

gen = fibonacci_infinite()

for num in gen:
    if num > 100:   # external stop condition
        break
    print(num)


print("# ------------- Lazy Log File Reader -------------- #")

# FILE PATH

base = Path(__file__).resolve().parent
input_file = base / "data" / "input" / "server.log"

for line in read_logs(input_file):
    print(line)


print("# ------------- CSV to Dictionary Converter -------------- #")

output = csv_to_dict_list("people.csv")
print(output)


print("# ------------- E-Commerce Cart Management -------------- #")


cart = {"apple": 2, "banana": 1, "mango": 5}
prices = {"apple": 100, "banana": 40, "mango": 60}

# 1. Total Bill
total_bill = sum(prices[item] * qty for item, qty in cart.items())
print("Total Bill:", total_bill)

# 2. Apply Discount using set()
unique_items = set(cart.keys())
if len(unique_items) > 2:
    discount = total_bill * 0.10
    total_bill -= discount
    print("Discount Applied: 10%")

print("Final Bill:", total_bill)

# 3. Items costing more than ₹50 each
costly_items = [item for item, price in prices.items() if price > 50]
print("Items costing > ₹50:", costly_items)
