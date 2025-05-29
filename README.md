# 🛒 Dmart Sales Analysis using PySpark

## 📌 Project Overview

This project focuses on building a data pipeline using **PySpark** to analyze sales data from Dmart. The objective is to integrate data from three sources—**products**, **sales transactions**, and **customers**—and perform insightful analytics using PySpark.

---

## 🛠️ Technologies Used

- Python
- PySpark

---

## 📂 Dataset

The datasets used in this project include:

- `products.csv`
- `sales.csv`
- `customer.csv`

These datasets are available at: [📁 Dataset Drive Link](https://drive.google.com/drive/folders/1Y7Q27S00milOYuPnO7NHLl3sQGd49Ng6?usp=sharing)

---

## 🧩 Problem Statement

Create a PySpark-based data pipeline that:

- Connects to and reads CSV data.
- Cleans and transforms the data.
- Performs joins across datasets.
- Answers key analytical business questions.

---

## 📋 Tasks Breakdown

### ✅ Task 1: Establish PySpark Connection

- Set up a SparkSession.
- Connect to local files and read them into DataFrames.

### ✅ Task 2: Load Data into PySpark DataFrames

- Load `products.csv`, `sales.csv`, and `customer.csv` into individual PySpark DataFrames.

### ✅ Task 3: Data Transformation and Cleaning

- Rename inconsistent columns.
- Handle null values.
- Cast data types appropriately.
- Join datasets on `Product ID` and `Customer ID`.

### ✅ Task 4: Data Analysis and Querying

Write PySpark code to answer the following questions:

1. What is the total sales for each product category?
2. Which customer has made the highest number of purchases?
3. What is the average discount given on sales across all products?
4. How many unique products were sold in each region?
5. What is the total profit generated in each state?
6. Which product sub-category has the highest sales?
7. What is the average age of customers in each segment?
8. How many orders were shipped in each shipping mode?
9. What is the total quantity of products sold in each city?
10. Which customer segment has the highest profit margin?

---

## 🔄 Project Structure

```bash
dmart-analysis/
│
├── data/
│   ├── products.csv
│   ├── sales.csv
│   └── customer.csv
│   
├── src/
│   ├── transformation.py
│   ├── spark.py
│   ├── schema.py
│   ├── load.py
│   └── analysis.py
|
├── main.py
│
├── README.md
```

## 🚀 How to Run

1. **Clone the repository:**

```bash
git clone https://github.com/your-username/dmart-analysis.git
cd dmart-analysis
```

2.**Run the main Python file**
```
Run the main Python file
```


