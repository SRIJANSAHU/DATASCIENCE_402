"""
Real Estate Price Normalization
"""

# imports
import numpy as np

# Dataset
dataset = np.array([[8500, 8700, 9100, 9500],
                    [7800, 7900, 8100, 8300],
                    [7200, 7300, 7600, 7900]])

print("Dataset:\n", dataset)
print("Dataset Shape(Rows & columns):\n", dataset.shape)

z_scaled_dataset = (dataset - dataset.mean(axis=1,keepdims=True)) / dataset.std(axis=1,keepdims=True)
print("Z-score scaled dataset:\n", z_scaled_dataset)

growth = (dataset[:,1:] - dataset[:,:-1]) / dataset[:,:-1] * 100
print("Growth (YoY):\n", growth)