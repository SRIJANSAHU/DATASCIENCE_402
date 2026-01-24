"""
Sales Analytics Dashboard
"""

# imports
import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt

# Path
input_path = Path(__file__).parent.joinpath("data","input","sales_data.csv")
# output path
output_path_region_summary = Path(__file__).parent.joinpath("data","output","region_summary.csv")
output_path_product_summary = Path(__file__).parent.joinpath("data","output","product_summary.csv")
output_path_top_5_products = Path(__file__).parent.joinpath("data","output","top5_products.csv")
output_path_monthly_sales = Path(__file__).parent.joinpath("data","output","monthly_sales.csv")

# read dataset
df_missing = pd.read_csv(input_path)
print("sales dataset:\n", df_missing)
print("Dataset info:\n", df_missing.info())

# Data cleaning
# Basic data summary
print("Data summary:\n", df_missing.describe())

# identification of missing data 
print("Missing data:\n",df_missing.isna().sum())

# since the data is skewed and standard deviation is high we will use median() to impute missing data
# 'Quantity'
df_missing['Quantity'] = df_missing['Quantity'].fillna(df_missing['Quantity'].median())

# for convert date to date-time
df_missing['Date'] = pd.to_datetime(df_missing['Date'], errors='coerce')
df_missing['Date'] = df_missing['Date'].fillna(method='ffill')
df_missing['Date'] = df_missing['Date'].fillna(method='bfill')

# handle missing categorical values
df_missing['Region'] = df_missing['Region'].fillna("Unknown")
df_missing['Product'] = df_missing['Product'].fillna("Unknown")

# 'Sales'
df_missing['Sales'] = df_missing['Quantity'] * (df_missing['Sales'].median() / df_missing['Quantity'].median())

# fill Profit with median profit margin
profit_margin = df_missing['Profit'].median() / df_missing['Sales'].median()
df_missing['Profit'] = df_missing['Profit'].fillna(df_missing['Sales'] * profit_margin)

# Remove duplicates
df_missing = df_missing.drop_duplicates()

print("-" * 25,"Cleaned Dataset","-" * 25)
print(df_missing)
print(df_missing.info())
print(df_missing.isna().sum())

print("-" * 25,"total sales and profit per region and product","-" * 25)

region_summary = df_missing.groupby('Region')[['Sales', 'Profit']].sum().reset_index()
product_summary = df_missing.groupby('Product')[['Sales', 'Profit']].sum().reset_index()

print("-" * 25,"Sales & Profit by region","-" * 25)
print(f"region summary:{region_summary}")

print("-" * 25,"Sales & Profit by Product","-" * 25)
print(f"product summary:{product_summary}")

# Top 5 products by profit margin
product_summary['Profit_Margin'] = product_summary['Profit'] / product_summary['Sales']
top_5_products = product_summary.sort_values(by='Profit_Margin', ascending=False)

print("-" * 25,"top 5 products by profit margin","-" * 25)
print(top_5_products)

# Monthly Sales Trend
df_cleaned = df_missing

df_cleaned['Month'] = df_cleaned['Date'].dt.to_period('M')

monthly_sales = df_cleaned.groupby('Month')['Sales'].sum()

plt.figure()
monthly_sales.plot()
plt.title("Monthly Sales Trend")
plt.xlabel("Month")
plt.ylabel("Total Sales")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Summary report

summary_report = {
    "Region_Summary": region_summary,
    "Product_Summary": product_summary,
    "Top5_Products": top_5_products,
    "Monthly_Sales": monthly_sales.reset_index()
}

# Save report
region_summary.to_csv(output_path_region_summary, index=False)
product_summary.to_csv(output_path_product_summary, index=False)
top_5_products.to_csv(output_path_top_5_products, index=False)
monthly_sales.reset_index().to_csv(output_path_monthly_sales, index=False)
