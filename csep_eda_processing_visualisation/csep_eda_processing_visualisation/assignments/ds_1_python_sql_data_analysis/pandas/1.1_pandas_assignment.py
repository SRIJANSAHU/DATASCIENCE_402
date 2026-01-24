"""
Hands-On Practice Problems in Pandas
"""

# imports

import pandas as pd
import numpy as np
import random
import matplotlib.pyplot as plt
from pathlib import Path
import csv

# 1.1 Basic Operations & Exploration
# using DataFrames
print("-" * 25,"Basic Operations & Exploration","-" * 25)

student_marks = {
    "ID" : np.arange(1,11),
    "Mathematics" : np.random.randint(0,100, size=10),
    "Science" : np.random.randint(0,100, size=10),
    "English" : np.random.randint(0,100, size=10),
    "Computer Science" : np.random.randint(0,100, size=10),
    "Hindi" : np.random.randint(0,100, size=10),
}

student_marks_data = pd.DataFrame(student_marks)

print("Top 5 rows:\n",student_marks_data.head())
print("bottom 5 rows:\n",student_marks_data.tail())
print(student_marks_data.shape)
student_marks_data.info()

average_marks = student_marks_data.mean(axis=1)
print("Average marks of each student:\n",average_marks)

# using Series

social_studies = pd.Series(data=np.random.randint(0,100, size=10), name= "Social studies")
print("Social studies marks:\n",social_studies)

avg_marks_social_studies = social_studies.mean()
print("Average marks of social studies:\n",avg_marks_social_studies)


# 1.2 Data Cleaning and Handling Missing Values
print("-" * 25,"Data Cleaning and Handling Missing Values","-" * 25)

# Dataset
employee_dataset = {
    "Employee_ID": [101, 102, 103, 104, 105, 106],
    "Name": ["Amit", "Sara", "John", "Priya", "Rahul", "Anita"],
    "Salary": [50000, np.nan, 60000, 55000, np.nan, 58000],
    "Department": ["HR", "it", "Finance", None, "IT", "hr"]
}

df_missing = pd.DataFrame(employee_dataset)
print("Employee Dataset:\n",df_missing)

# check for all missing values
missing_values = df_missing.isnull().sum()
print("missing values:\n",missing_values)

# fill null values with mean
df_filled = df_missing.fillna(df_missing.mean(numeric_only=True))
print("Filled data with mean:\n", df_filled)

# drop rows with department as null value
df_filled = df_filled.dropna(axis=0)
print("Dataset after dropping rows with department as Null:\n", df_filled)

# standardize "Department" column
df_filled['Department'] =  df_filled['Department'].str.upper()
print("Dataset after 'Department' column is standardized:\n", df_filled)

# 1.3 Data Filtering and Conditional Selection
print("-" * 25,"Data Filtering and Conditional Selection","-" * 25)

product_data = {
    "Category": ["Electronics", "Clothing", "Electronics", "Home", "Clothing", "Electronics", "Home"],
    "Price": [1200, 800, 450, 600, 300, 1500, 700],
    "Rating": [4.5, 4.2, 3.8, 4.1, 3.9, 4.8, 4.0]
}

product_df = pd.DataFrame(product_data)
print("Product Data:\n", product_df)

# get all rows where Price > 500 and Rating > 4.0
print("Rows where Price >500 & Rating > 4.0:\n", product_df[(product_df['Price'] > 500) & (product_df['Rating'] > 4.0)])

# Show only Category and Price columns for “Electronics”
selected_subset_df = product_df.loc[product_df["Category"] == "Electronics", ["Category","Price"]]
print("'Category' specific subset:\n",selected_subset_df)

# Count number of products in each 'category'
category_count = product_df.groupby("Category")["Price"].nunique()
print("Number of products in each category:\n", category_count)


# 1.4 GroupBy and Aggregations
print("-" * 25,"GroupBy and Aggregations","-" * 25)

# Dataset
store_dataset = {
    "Store": ["A", "A", "A", "B", "B", "B", "C", "C", "C"],
    "Month": ["Jan", "Feb", "Mar", "Jan", "Feb", "Mar", "Jan", "Feb", "Mar"],
    "Sales": [20000, 22000, 25000, 18000, 21000, 23000, 15000, 17000, 19000],
    "Profit": [4000, 4500, 5000, 3500, 4200, 4800, 3000, 3600, 3900]
}

store_df = pd.DataFrame(store_dataset)

# Total and Average sales
sales = store_df.groupby("Store").agg({
    "Sales" : ["sum","mean"]
})
print("Total & average sales grouped by 'Store':\n", sales)

# Month in which store had highest profit

profit_index = store_df.groupby("Store")["Profit"].idxmax()
profit = store_df.loc[profit_index, ["Store", "Month", "Profit"]]
print("Month having highest profit per store:\n", profit)

# Sort stores by average profit
avg_profit_sorted = (store_df.groupby("Store")["Profit"].mean().sort_values(ascending=False))
print("Average profit sorted by Store:\n", avg_profit_sorted)

# 1.5 Applying Custom Functions
print("-" * 25,"Applying Custom Functions","-" * 25)

student_marks_dataframe = pd.DataFrame(student_marks)
print("Student Dataset:\n",student_marks_dataframe)

# Grading students
# Adding a new column for total marks
student_marks_dataframe["Total marks"] = student_marks_dataframe["Mathematics"] + student_marks_dataframe["Computer Science"] + student_marks_dataframe["Hindi"] + student_marks_dataframe["English"] + student_marks_dataframe["Science"]
print("After adding total marks column:\n", student_marks_dataframe)

# Grading function(Grading logic)
def assign_marks(marks : int):
    """Assign grades to students"""
    if marks >= 400:
        return "A"
    elif marks >= 300:
        return "B"
    elif marks >= 200:
        return "C"
    elif marks >= 150:
        return "D"
    else:
        return "F"

# Result function(Pass/ Fail : logic) 
def assign_result(grade : int):
    """Assigns Pass & Fail"""
    if grade == "F":
        return "Fail"
    else:
        return "Pass"


# adding grade column
student_marks_dataframe["Grades"] = student_marks_dataframe["Total marks"].apply(assign_marks)
print("Grading:\n", student_marks_dataframe)

# adding result column
student_marks_dataframe["Result"] = student_marks_dataframe["Grades"].apply(assign_result)
print("Results:\n", student_marks_dataframe)

# Normalization of marks
subjects = ["Mathematics", "Computer Science", "Hindi", "English", "Science","Total marks"]

min_marks = student_marks_dataframe[subjects].min(axis=1)
max_marks = student_marks_dataframe[subjects].max(axis=1)

student_marks_dataframe[subjects] = student_marks_dataframe[subjects].apply(lambda x : (x - min_marks)/(max_marks - min_marks))
print("Normalized Dataset:\n", student_marks_dataframe)


# 1.6 Working with Dates and Times
print("-" * 25,"Working with Dates and Times","-" * 25)

# Dataset
stock_dataset = {
    "Date": [
        "2022-01-03", "2022-01-04", "2022-01-05",
        "2022-02-01", "2022-02-02",
        "2022-03-01", "2022-03-02"
    ],
    "Open": [100, 102, 101, 105, 107, 110, 112],
    "Close": [102, 101, 104, 107, 106, 112, 115],
    "Volume": [12000, 15000, 13000, 16000, 14000, 17000, 18000]
}

stock_dataframe = pd.DataFrame(stock_dataset)
print("Stock Dataset:\n", stock_dataframe)

# convert 'Date' column to datetime
stock_dataframe["Date"] = pd.to_datetime(stock_dataframe["Date"])
print("Converted Datetime column:\n", stock_dataframe["Date"])

# set Date as index
stock_df_idx = stock_dataframe.set_index("Date")
print("DataFrame with index as ('Date'):\n", stock_df_idx)

# Daily returns
stock_df_idx["Daily Returns"] = (stock_df_idx["Close"] - stock_df_idx["Open"]) * stock_df_idx["Volume"]
print("Daily Returns:\n",stock_df_idx)

jan_data = stock_df_idx.loc["2022-01"]
print("Jaunary data:\n", jan_data)

feb_data = stock_df_idx.loc["2022-02"]
print("february data:\n", feb_data)


# 1.7 Merging, Joining & Concatenation
print("-" * 25,"Merging, Joining & Concatenation","-" * 25)

# Datasets
df_customers = pd.DataFrame({
    "Customer_ID": [1, 2, 3, 4],
    "Customer_Name": ["Amit", "Sara", "John", "Priya"],
    "City": ["Delhi", "Mumbai", "London", "Bangalore"]
})
print("Customer Dataset:\n", df_customers)

df_orders = pd.DataFrame({
    "Order_ID": [101, 102, 103],
    "Customer_ID": [1, 2, 5],
    "Product": ["Laptop", "Mobile", "Tablet"],
    "Amount": [70000, 30000, 20000]
})
print("Order Dataset:\n", df_orders)

# inner join and outer join

inner_join = pd.merge(
    df_customers,
    df_orders,
    on = "Customer_ID",
    how = "inner"
)

outer_join = pd.merge(
    df_customers,
    df_orders,
    on = "Customer_ID",
    how = "outer"
)

left_join = pd.merge(
    df_customers,
    df_orders,
    on = "Customer_ID",
    how = "left"
)

print("Inner Join:\n", inner_join)
print("Outer Join:\n", outer_join)

# Vertical and Horizontal concatenation

vertical_concatenation = pd.concat(
    [df_customers,df_orders],
    axis=0,
    ignore_index=True
)
print("Vertical concatenation:\n", vertical_concatenation)

horizontal_concatenation = pd.concat(
    [df_customers,df_orders],
    axis=1,
    ignore_index=True
)
print("Horizontal concatenation:\n", horizontal_concatenation)

# Handle missing data

no_purchase_customers = left_join[left_join["Order_ID"].isna()]
print("Customers with no purchases:\n", no_purchase_customers)


# 1.8 Pivot Tables and Crosstabs
print("-" * 25,"Pivot Tables and Crosstabs","-" * 25)

# Dataset
data = {
    "Region": ["North", "North", "South", "South", "East", "East", "West", "West"],
    "Product": ["Laptop", "Mobile", "Laptop", "Tablet", "Mobile", "Tablet", "Laptop", "Mobile"],
    "Sales": [50000, 30000, 45000, 20000, 28000, 22000, 52000, 31000]
}

df_data = pd.DataFrame(data)
print("Region Sales Dataset:\n",df_data)

# pivot table: Total sales per region per product
pivot_table = pd.pivot_table(
    df_data,
    values="Sales",
    index="Region",
    columns="Product",
    aggfunc="sum" 
)
print("Total sales per region per product:\n", pivot_table)

# Add Margins(Total)
pivot_with_margins = pd.pivot_table(
    df_data,
    values="Sales",
    index="Region",
    columns="Product",
    aggfunc="sum",
    margins="True",
    margins_name="Total"
)
print("Pivot table with margin:\n", pivot_with_margins)

# count occurrences of product categories per region
product_counts = pd.crosstab(
    df_data["Region"],
    df_data["Product"]
)
print("Count product occurences per region:\n", product_counts)


# 1.9 Data Visualization using Pandas
print("-" * 25,"Data Visualization using Pandas","-" * 25)

monhly_sales_data = {
    "Month": ["Jan", "Jan", "Feb", "Feb", "Mar", "Mar"],
    "Store": ["A", "B", "A", "B", "A", "B"],
    "Sales": [20000, 18000, 22000, 21000, 25000, 24000],
    "Profit": [4000, 3500, 4500, 4200, 5200, 5000],
    "Region": ["North", "South", "North", "South", "North", "South"]
}
df_monthly_sales = pd.DataFrame(monhly_sales_data)
print("Monthly Sales Data\n", df_monthly_sales)

# Plot sales trends for each store
trend_plot = df_monthly_sales.groupby(["Month","Store"])["Sales"].sum().unstack().plot(title="Monthly sales trend by store")

plt.xlabel("Month")
plt.ylabel("Sales")
#plt.show()

# Total Profit by Region
total_profit_by_region = df_monthly_sales.groupby("Region")["Profit"].sum().plot(kind="bar", title="Total Profit by Region")

plt.xlabel("Region")
plt.ylabel("Profit")
#plt.show()

# Sales Distribution
sales_distribution = df_monthly_sales["Sales"].plot(kind="hist", bins=5, title="Sales Distribution")

plt.xlabel("Sales")
plt.ylabel("Frequency")
#plt.show()


# 1.9 Real-world Data Wrangling Challenge
print("-" * 25,"Titanic survivor analysis","-" * 25)

# path
input_path = Path(__file__).parent.joinpath("data", "input", "Titanic_Train.csv")

# Dataset
df_titanic = pd.read_csv(input_path)
print("Titanic dataset:\n", df_titanic)

# basic information about dataset
print("Basic information:\n",df_titanic.info())

# missing values
print("Missing or null values:\n",df_titanic.isna().sum())

# Basic EDA
print("Basic EDA:\n",df_titanic.describe())

# we see that Age has minimum value as 0.42 that should not be possible
print("Age minimum and maximum value:", df_titanic["Age"].min(), df_titanic["Age"].max())

print("-" * 25,"'Age' column analysis","-" * 25)

# age distribution
print("Skewness:\n", df_titanic["Age"].skew())
print("Observation: slight positive skew i.e. children had more probability of survival")
print("kurtosis:\n", df_titanic["Age"].kurtosis())
print("Observation: platykurtic distribution i.e. tailedness is more (more values towards the end)")

# data cleaning
# since data is skewed we will use median value to replace the missing ones
df_titanic_missing = df_titanic
df_titanic_missing["Age"] = df_titanic_missing["Age"].fillna(df_titanic["Age"].median())

# for 'Embarked' column
df_titanic_missing['Embarked'] = df_titanic_missing['Embarked'].fillna(df_titanic_missing['Embarked'].mode()[0])

# for 'Fare' column
df_titanic_missing['Fare'] = df_titanic_missing['Fare'].fillna(df_titanic_missing['Fare'].median())

print("Dataframe after handling missing values in age column\n",df_titanic_missing.describe(), df_titanic_missing.info())

# Convert data types
df_titanic_missing['Survived'] = df_titanic_missing['Survived'].astype(int)
df_titanic_missing['Pclass'] = df_titanic_missing['Pclass'].astype(int)
print(df_titanic_missing.info())

# new features
# Age group
df_titanic_missing['AgeGroup'] = pd.cut(df_titanic_missing['Age'], bins=[0, 12, 20, 40, 60, 100], labels=['Child', 'Teen', 'Adult', 'Middle-aged', 'Senior'])

# Family size
df_titanic_missing['FamilySize'] = df_titanic_missing['SibSp'] + df_titanic_missing['Parch'] + 1

# Grouping & Visualization

# Survival rate by AgeGroup
survival_by_age = df_titanic_missing.groupby('AgeGroup')['Survived'].mean()

plt.figure()
survival_by_age.plot(kind='bar')
plt.title("Survival Rate by age group")
plt.ylabel("Survival rate")
plt.xlabel("Age Group")
plt.show()

# Survival rate by Class
survival_by_class = df_titanic_missing.groupby('Pclass')['Survived'].mean()

plt.figure()
survival_by_class.plot(kind='bar')
plt.title("Survival rate by Passenger Class")
plt.ylabel("Survival Rate")
plt.xlabel("Passenger Class")
plt.show()

# Summary

summary = {
    "Average Age" : df_titanic_missing['Age'].mean(),
    "Average fare" : df_titanic_missing['Fare'].mean(),
    "Overall Survival Rate" : df_titanic_missing['Survived'].mean(),
    "Highest Survival Class" : survival_by_class.idxmax(),
    "Highest Survival Age Group" : survival_by_age.idxmax()
}

print("-" * 25,"Summary\n","-" * 25)

print(summary)