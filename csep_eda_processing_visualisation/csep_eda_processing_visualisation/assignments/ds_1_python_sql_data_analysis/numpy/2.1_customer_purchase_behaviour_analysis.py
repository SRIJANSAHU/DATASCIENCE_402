"""
Customer Purchase Behaviour Analysis
"""

# imports

import numpy as np
import random

# Generate synthetic data

np.random.seed(42)
customers = np.arange(1, 11)
purchases = np.random.randint(10, 100, size = 10)
avg_order_value = np.random.randint(20, 500, size = 10)
returns = np.random.randint(0, 5, size = 10)

data = np.column_stack((customers, purchases, avg_order_value, returns))
print("Data:\n", data)
print(np.shape(data))

# Total revenue per customer

total_revenue_per_customer = purchases * avg_order_value
print("Total revenue per customer:\n", total_revenue_per_customer)

# Losses because of Returns

return_loss_rate = 0.05
return_loss = returns * avg_order_value * return_loss_rate
print("Loss Due to Returns:\n", return_loss)

# Adjust total revenue based on returns-losses

adjusted_revenue = total_revenue_per_customer - return_loss
print("adjusted revenue:\n", adjusted_revenue)

# top customers based on adjusted-revenue

top_customers_indices = np.argsort(adjusted_revenue)[-5:][::-1]
top_customers = data[top_customers_indices]
print("Top 5 customers:\n", top_customers)

# Descriptive stastics
mean = np.mean(adjusted_revenue)
standard_deviation = np.std(adjusted_revenue)
median = np.median(adjusted_revenue)
IQR = np.percentile(adjusted_revenue, 75) - np.percentile(adjusted_revenue, 25)

print("-" * 10, "DESCRIPTIVE STATISTICS ABOUT ADJUSTED REVENUE","-"*10 )
print("mean:\n",mean)
print("standard deviation:\n",standard_deviation)
print("median:\n",median)
print("Inter quartile range:\n",IQR)