"""NumPy handson"""

# imports

import numpy as np
import random

# mainline execution

# --------  Company sales report --------

# input
online_sales = np.array([220, 330, 180, 260, 300, 270, 350])
store_sales = np.array([120, 150, 100, 180, 200, 160, 210])

print("#--------  Company sales report --------#\n")

total_sales = online_sales + store_sales
print("Total sales(day wise(in $)):", total_sales)

average_sales = round(total_sales.mean(),2) # axis = 1 means along the row
print("Average sales for the week(in $):", average_sales)

days_above_avg = np.where(total_sales > average_sales, "Yes", "No")
print("days above average sales:", days_above_avg)


# --------  Statistical Analysis of Student Scores --------

# input
student_scores = np.random.randint(0,101,size=(10,5)) # generates data for dimensions 10x5 and [0,100] inclusive
print("student scores:\n",student_scores)

print("#--------  Statistical Analysis of Student Scores --------#\n")

mean = np.mean(student_scores, axis=0) # axis = 0 : along the column
median = np.median(student_scores, axis=0)
std = np.std(student_scores, axis=0)

print(f"Subject-wise mean:{mean}")
print(f"Subject-wise median:{median}")
print(f"Subject-wise standard deviation:{std}")

# --------  Data cleaning: Handling Missing Data --------

print("#--------  Data cleaning: Handling Missing Data --------#\n")
#NaN is NOT equal to anything — not even itself

# input
temps = np.array([21, 23, np.nan, 25, 24, np.nan, 22, 26, 27, np.nan, 28, 29, 24, 23, 25, 26, np.nan, 27, 28, 29]) 
print("Original data:",temps)

invalid_index = np.isnan(temps)
print("Invalid Indexes(True: null values):\n",invalid_index)

mean_temp = np.nanmean(temps) # nan safe mean
# create a copy
imputed_temps = temps
imputed_temps[np.isnan(temps)] = np.nanmean(temps)
print("\nData with replaced mean:\n",imputed_temps)

# Verification

print("\nMissing Values in Dataset:\n",np.isnan(imputed_temps))

# --------  E-Commerce Profit Analysis --------
print("#--------  E-Commerce Profit Analysis --------#\n")

prices = np.array([120, 250, 80, 150, 200]) 
quantities = np.array([10, 5, 15, 8, 12]) 
discount = np.array(0.1) 

total_revenue = prices * quantities
print("Total revenue:\n",total_revenue)

discounted_revenue = discount * total_revenue  # 1x1 array braodcasted 1x5 array
print("Total revenue after discount:\n",discounted_revenue)

# --------  Data Normalization for ML  --------
print("#--------  Data Normalization for ML  --------#\n")

X = np.array([[20, 30000], [25, 45000], [30, 50000], [35, 60000], [40, 80000]])

print("access first element:",X[0])
print("access first index of first element:",X[0,0])
print("access second index of first element:",X[0,1])

X_scaled= (X - X.min(axis = 0)) / (X.max(axis = 0) - X.min(axis = 0))
print("\n Scaled X b/w(0 & 1):\n",X_scaled)

Z_score_standardized_X = (X_scaled - np.mean(X_scaled)) / np.std(X_scaled)
Z_score_standardized_X = np.array(Z_score_standardized_X)
print("\nZ-score Standardization:\n",Z_score_standardized_X)

# checking if Z-score is in between -1 and 1 (b/w -1 s.d. and +1 s.d.) to detect potential outliers

# --------  Linear Algebra in Data Science  --------
print("#--------  Linear Algebra in Data Science  --------#\n")


W = np.array([[0.2, 0.4, 0.6],[0.1, 0.3, 0.5]])
X = np.array([10, 20, 30])

dot_product = np.dot(W,X)
print("dot product of W & X:\n", dot_product)

W_T = W.transpose()
print("W_transpose:\n",W_T)

# --------  Customer Segmentation Metrics  --------
print("#--------  Customer Segmentation Metrics  --------#\n")

customers = np.array([[50, 30],[70, 40],[80, 20],[60, 60],[90, 80]])
print("Customer Dataset:\n", customers)
print("Index [0]:",customers[0])
print("Index [0,1]:",customers[0,0])

differences = customers[:,np.newaxis,:] - customers[np.newaxis,:,:]

# customers[:,np.newaxis,:] -> (5, 2) → (5, 1, 2)
# customers[np.newaxis,:,:] -> (5, 2) → (1, 5, 2)
# resulting shape -> (5, 5, 2)

distances = np.sqrt(np.sum(differences ** 2, axis=2)) # axis = -1 also works

print("Pairwise distances:\n", distances)




# --------  Stock Market Analysis  --------
print("#--------  Stock Market Analysis  --------#\n")

prices = np.array([120, 125, 123, 128, 130])
shares_holding = np.array([100])

daily_returns = np.array(([(prices[i] - prices[i - 1]) for i in range(1,len(prices))])*shares_holding)
print("daily returns:\n", daily_returns)

cummulative_returns = np.cumsum(daily_returns)
print("cummulative returns over the period:\n", cummulative_returns)

average_daily_returns = np.average(daily_returns)
print("average daily returns:\n",average_daily_returns)

volatility = np.std(prices)
print("volatility:\n",volatility)

# --------  Image Normalization  --------
print("#--------  Image Normalization  --------#\n")

image = np.random.randint(0, 256, size=(28, 28))
print("size of image:\n",np.size(image))
print("dimensions:\n",np.shape(image))

normalized_image = (image - np.min(image)) / (np.max(image) - np.min(image))
print("Normalized image:\n", normalized_image)
# print(type(normalized_image))

mean_pixel_substraction = normalized_image - np.mean(normalized_image)
print("matrix after mean pixel substraction:\n", mean_pixel_substraction)

image_clipped = np.clip(mean_pixel_substraction, 0, 1)
print("clipped image matrix [0,1]:\n", image_clipped)

# --------  Housing Data Simulation and Correlation  --------
print("\n#--------  Housing Data Simulation and Correlation  --------#\n")

np.random.seed(42)
area = np.random.randint(1000, 4000, 100)
bedrooms = np.random.randint(1, 6, 100)
price = 50 + 0.05 * area + 10 * bedrooms + np.random.randn(100) * 10

data = np.column_stack((area,bedrooms,price))
correlation_matrix = np.corrcoef(data, rowvar=False) # column:variable , row:observations 
print("correlation b/w area, bedrooms and price:\n",correlation_matrix)

top_5_houses_indices = np.argsort(price)[-5:][::-1] # extract last 5 and reverse
top_5_houses = data[top_5_houses_indices]
print("top 5 houses:\n", top_5_houses)

# mean data normalization

mean = np.mean(data, axis=0)
range = np.max(data, axis=0) - np.min(data, axis=0)
mean_normalized_data = (data - mean) / range
print("mean normalized data:\n", mean_normalized_data)