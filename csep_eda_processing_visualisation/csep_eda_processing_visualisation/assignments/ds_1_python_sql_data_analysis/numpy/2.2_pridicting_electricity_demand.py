"""
Predicting Electricity Demand
"""

# imports
import numpy as np

# Dataset simulation

np.random.seed(0) 
hours = np.arange(1, 25) # 24 hours 
usage = np.random.randint(200, 500, size=24) # MW usage per hour 
temperature = np.random.randint(10, 35, size=24) # Celsius 

data = np.column_stack((hours, usage, temperature))

print("Data:\n",data)
print("Shape of Data(Rows & columns):\n",data.shape)

mean_usage = usage.mean()
print("Mean(usage):\n",mean_usage)
usage_standard_deviation = usage.std()
print("standard deviation(usage):\n",usage_standard_deviation)

z_scaled_usage = ( usage - mean_usage ) / usage_standard_deviation
print("usage (Z-scaled):\n", z_scaled_usage)

impact_factor = usage / temperature
print("impact factor:\n",impact_factor)
print(type(impact_factor))

combined_features = np.column_stack((hours,z_scaled_usage,impact_factor))
print("combined features per hour:\n",combined_features)