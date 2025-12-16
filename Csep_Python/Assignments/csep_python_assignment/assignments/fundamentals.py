"""
Docstring for csep_python_assignment.assignments.fundamentals
"""
# imports
from pathlib import Path
import json
import csv
from datetime import datetime

# ------------- Temperature Converter -------------- #

def convert_to_fahrenheit(temperature_list):
    """
    Docstring for convert_to_fahrenheit

    Formula:
    F = (C *(9/5)) + 32
    """
    # ---- input validation ---- #  
    if not isinstance(temperature_list, list):
        raise TypeError("temperature_list must be a list")
    
    for item in temperature_list:
        if not isinstance(item, (int,float)):
            raise ValueError(f"All items must be numeric values. Invalid item: {item}")
    # ---- conversion ---- #   
    converted_list = list(
        map(lambda t: (t*(9/5))+32 , temperature_list)
    )
    return converted_list


# ------------- Student Grades Processor -------------- #

def top_scorers(student_dict):
    """Find student(s) with highest score"""
    max_score = max(student_dict.values())
    toppers = []

    # Loop through dictionary to collect all toppers
    for name, score in student_dict.items():
        if score == max_score:
            toppers.append(name)

    return toppers, max_score

def check_results(student_dict, cutoff=40):
    """Find pass/fail for each student"""
    results = {}

    for name, score in student_dict.items():
        if score >= cutoff:
            results[name] = "Pass"
        else:
            results[name] = "Fail"
    
    return results

# ------------- Word Counter in a Text File -------------- #

def word_counter():
    try:
        # Resolve project path
        base_path = Path(__file__).resolve().parent

        # Paths for input and output
        input_file = base_path / "data" / "input" / "sample.txt"
        output_file = base_path / "data" / "output" / "result_word_count.txt"

        # Read file
        text = input_file.read_text().lower()

        # Count words 
        word_counts = {}
        for word in text.split():
            word = word.strip(",.!?;:")
            word_counts[word] = word_counts.get(word, 0) + 1

        # Sort by frequency (descending)
        sorted_counts = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)

        # Write results
        with output_file.open("w") as f:
            for word, count in sorted_counts:
                f.write(f"{word}: {count}\n")

        print(f"Word count saved to: {output_file}")

    except FileNotFoundError:
        print("Input file not found! Please check the path or filename.")

    except Exception as e:
        print("An error occurred:", e)

# ------------- JSON-based Contact Book  -------------- #

def get_json_file():
    """Helper: Get JSON file path"""
    base_path = Path(__file__).resolve().parent
    return base_path / "data" / "output" / "contacts.json"

def load_contacts():
    """Load contacts from JSON"""
    json_file = get_json_file()

    # Create file if it doesn't exist
    if not json_file.exists():
        json_file.write_text("[]")

    try:
        return json.loads(json_file.read_text())
    except json.JSONDecodeError:
        # If corrupted, reset file
        json_file.write_text("[]")
        return []
    
def save_contacts(contacts):
    """Save contacts"""
    json_file = get_json_file()
    json_file.write_text(json.dumps(contacts, indent=4))

def add_contact(name, email, phone):
    """Add new contact"""
    contacts = load_contacts()

    new_contact = {
        "name": name,
        "email": email,
        "phone": phone
    }

    contacts.append(new_contact)
    save_contacts(contacts)
    print(f"Contact '{name}' added successfully!")

def search_contact(keyword):
    """Search contact by name"""
    contacts = load_contacts()

    # Case-insensitive search
    results = [
        c for c in contacts
        if keyword.lower() in c["name"].lower()
    ]

    return results

def list_contacts():
    """List all contacts"""
    return load_contacts()

# ------------- CSV Employee Salary Analyzer  -------------- #

def get_paths():
    """Helper: Get input & output file paths"""
    base = Path(__file__).resolve().parent
    input_file = base / "data" / "input" / "employees.csv"
    output_file = base / "data" / "output" / "dept_salary_avg.csv"
    return input_file, output_file

def analyze_salaries():
    """compute avg salary per department"""
    input_file, output_file = get_paths()

    dept_salary = {}
    dept_count = {}

    try:
        with input_file.open() as f:
            reader = csv.DictReader(f)

            for row in reader:
                dept = row["Department"].strip()
                salary_raw = row["Salary"].strip()

                try:
                    salary = float(salary_raw)
                except Exception:
                    # Skip rows with invalid/missing salary
                    print(f"Skipping invalid salary value: {salary_raw}")
                    continue

                # Build department salary list
                dept_salary.setdefault(dept, 0)
                dept_count.setdefault(dept, 0)

                dept_salary[dept] += salary
                dept_count[dept] += 1

    except FileNotFoundError:
        print("employees.csv not found! Check your path.")
        return

    # Compute averages
    averages = {
        dept: dept_salary[dept] / dept_count[dept]
        for dept in dept_salary
        if dept_count[dept] > 0
    }

    # Write CSV output
    with output_file.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Department", "AverageSalary"])

        for dept, avg_salary in averages.items():
            writer.writerow([dept, round(avg_salary, 2)])

    print(f"Results saved to: {output_file}")

# ------------- Binary File Image Copier  -------------- #

def copy_binary_file():
    try:
        # Resolve base path 
        base = Path(__file__).resolve().parent

        # Input and output files
        input_file = base / "data" / "input" / "large_sample.bin"
        output_file = base / "data" / "output" / "copied_sample. bin"

        # Chunk size in bytes
        chunk_size = 4096  # 4 KB

        # Check file exists
        if not input_file.exists():
            raise FileNotFoundError("Input file not found!")

        # Get total size for progress
        total_size = input_file.stat().st_size
        copied = 0

        # Open both files in binary mode
        with input_file.open("rb") as infile, output_file.open("wb") as outfile:

            while True:
                chunk = infile.read(chunk_size)

                # If no more data, stop
                if not chunk:
                    break

                outfile.write(chunk)
                copied += len(chunk)

                # Progress percentage
                percent = (copied / total_size) * 100
                print(f"Copied {copied} bytes ({percent:.2f}%)")

        print("\n File copied successfully!")
        print(f"Output saved at: {output_file}")

    except FileNotFoundError:
        print("Error: Input file does not exist.")

    except PermissionError:
        print("Error: Access denied while reading or writing.")

    except Exception as e:
        print(f"Unexpected error: {e}")

# ------------- Number Analysis Tool  -------------- #

def get_numbers_from_user():
    user_input = input("Enter numbers separated by spaces: ")

    numbers = []
    for item in user_input.split():
        try:
            numbers.append(int(item))
        except ValueError:
            print(f"Skipping invalid input: {item}")
    return numbers

def compute_mean(nums):
    return sum(nums) / len(nums) if nums else None

def compute_median(nums):
    if not nums:
        return None
    nums = sorted(nums)
    n = len(nums)
    mid = n // 2

    if n % 2 == 0:
        return (nums[mid - 1] + nums[mid]) / 2
    else:
        return nums[mid]
    
def compute_mode(nums):
    if not nums:
        return None
    freq = {}
    for n in nums:
        freq[n] = freq.get(n, 0) + 1

    max_freq = max(freq.values())
    modes = [num for num, c in freq.items() if c == max_freq]

    return modes

def is_prime(n):
    """Prime check"""
    if n < 2:
        return False
    return all(n % i != 0 for i in range(2, int(n**0.5) + 1))

def get_primes(nums):
    return list(filter(lambda x: is_prime(x), nums))

# ------------- Weather Data Processor  -------------- #

def get_paths_weather():
    base = Path(__file__).resolve().parent
    input_file = base / "data" / "input" / "weather.csv"
    output_file = base / "data" / "output" / "weather_summary.json"
    return input_file, output_file

def process_weather():
    input_file, output_file = get_paths_weather()

    try:
        # ----- Read CSV -----
        weather_data = []
        with input_file.open() as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Convert numbers with try-except
                try:
                    temp = float(row["Temperature"])
                    rain = float(row["Rainfall"])
                except ValueError:
                    print(f"Skipping invalid row: {row}")
                    continue

                weather_data.append({
                    "Date": row["Date"],
                    "Temperature": temp,
                    "Rainfall": rain
                })

        if not weather_data:
            print("No valid weather data found.")
            return

        # ----- Find hottest day -----
        hottest = max(weather_data, key=lambda x: x["Temperature"])

        # ----- Find wettest day -----
        wettest = max(weather_data, key=lambda x: x["Rainfall"])

        # ----- Prepare summary -----
        summary = {
            "HottestDay": hottest,
            "WettestDay": wettest,
            "TotalDaysProcessed": len(weather_data)
        }

        # Ensure output directory exists
        output_file.parent.mkdir(parents=True, exist_ok=True)

        # ----- Write JSON -----
        with output_file.open("w") as f:
            json.dump(summary, f, indent=4)

        print(f"Weather summary saved at: {output_file}")

    except FileNotFoundError:
        print("weather.csv not found.")
    except PermissionError:
        print("Permission denied while reading or writing file.")
    except Exception as e:
        print("Unexpected error:", e)

# ------------- Log Parser -------------- #

def get_paths_log():
    base = Path(__file__).resolve().parent
    input_file = base / "data" / "input" / "server.log"

    # Timestamped output file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = base / "data" / "output" / f"errors_warnings_{timestamp}.log"

    return input_file, output_file

def parse_logs():
    input_file, output_file = get_paths_log()

    try:
        # --- Read entire log file ---
        log_text = input_file.read_text().splitlines()

        # --- Use filter + lambda to extract ERROR & WARNING ---
        filtered_lines = list(
            filter(lambda line: "ERROR" in line or "WARNING" in line, log_text)
        )

        # Create output folder if missing
        output_file.parent.mkdir(parents=True, exist_ok=True)

        # --- Write filtered lines to new file ---
        output_file.write_text("\n".join(filtered_lines))

        print(f"Extracted {len(filtered_lines)} lines.")
        print(f"Output saved to: {output_file}")

    except FileNotFoundError:
        print("Error: server.log not found.")

    except PermissionError:
        print("Error: Access denied to file.")

    except Exception as e:
        print("Unexpected error:", e)

# mainline execution

print("# ------------- Temperature Converter -------------- #")

# input
temperature_list = [23.5,14.2,44.2,45.6,32.12] # in celsius
#temperature_list = ['asd',23.5,14.2,44.2,45.6,32.12] # for error

result = convert_to_fahrenheit(temperature_list)
print("Converted list in Fahrenheit:", result)

print("# ------------- Student Grades Processor -------------- #")

# input
students = {
    "Alice": 85,
    "Bob": 39,
    "Charlie": 92,
    "David": 56,
    "Eva": 92
}

toppers, highest = top_scorers(students)
print("Highest Score:", highest)
print("Top Scorer(s):", toppers)

print("\nPass/Fail Results:")
results = check_results(students)
for name, status in results.items():
    print(f"{name}: {status}")

print("# ------------- Word Counter in text file -------------- #")

word_count = word_counter()
print("Result of word count :\n",word_count)

print("# ------------- JSON contact book -------------- #")

add_contact("Alice", "alice@example.com", "9876543210")
add_contact("Bob", "bob@example.com", "9991112233")

# Search example
print("\nSearch results for 'bo':")
print(search_contact("bo"))

# List all contacts
print("\nAll Contacts:")
print(list_contacts())

print("# ------------- CSV Employee Salary Analyzer -------------- #")
analyzed_salaries = analyze_salaries()
print("Analyzed salaries: \n",analyze_salaries)

print("# ------------- Binary File Image Copier -------------- #")
#binary_operation_result = copy_binary_file()
#print("Binary operation", binary_operation_result)

print("# ------------- Number Analysis Tool -------------- #")
#nums = get_numbers_from_user()

#print("\nValid Numbers:", nums)
#print("Mean:", compute_mean(nums))
#print("Median:", compute_median(nums))
#print("Mode:", compute_mode(nums))
#print("Prime Numbers:", get_primes(nums))

print("# ------------- Weather Data Processor -------------- #")
process_weather()

print("# ------------- Log Parser -------------- #")
parse_logs()

