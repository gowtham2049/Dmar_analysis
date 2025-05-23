from pyspark.sql.functions import col, sum, avg, countDistinct, count, desc

# What is the total sales for each product category?
# Which customer has made the highest number of purchases?
# What is the average discount given on sales across all products?
# How many unique products were sold in each region?
# What is the total profit generated in each state?
# Which product sub-category has the highest sales?
# What is the average age of customers in each segment?
# How many orders were shipped in each shipping mode?
# What is the total quantity of products sold in each city?
# Which customer segment has the highest profit margin?

def analysis(df):

    print("1.Total sales for each product category")
    df.groupBy("Category") \
    .agg(sum("Sales").alias("Total_Sales")) \
    .orderBy(desc("Total_Sales")) \
    .show()

    print("2.Customer who made the highest number of purchases")
    df.groupBy("Customer_ID", "Customer_Name") \
    .agg(count("Order_Line").alias("Purchase_Count")) \
    .orderBy(desc("Purchase_Count")) \
    .show(1)

    print("3.Average discount given on sales across all products")
    df.select(avg("Discount").alias("Average_Discount")).show()

    print("4.Unique products were sold in each region")
    df.groupBy("Region") \
    .agg(countDistinct("Product_ID").alias("Unique_Products")) \
    .orderBy("Unique_Products") \
    .show()

    print("5.Total profit generated in each state")
    df.groupBy("State") \
    .agg(sum("Profit").alias("Total_Profit")) \
    .orderBy(desc("Total_Profit")) \
    .show()

    print("6.Product sub-category has the highest sales")
    df.groupBy("Sub_Category") \
    .agg(sum("Sales").alias("Total_Sales")) \
    .orderBy(desc("Total_Sales")) \
    .show(1)

    print("7.the average age of customers in each segment")
    df.groupBy("Segment") \
    .agg(avg("Age").alias("Average_Age")) \
    .orderBy("Segment") \
    .show()

    print("8.No of orders were shipped in each shipping mode")
    df.groupBy("Ship_Mode") \
    .agg(countDistinct("Order_ID").alias("Orders_Shipped")) \
    .orderBy(desc("Orders_Shipped")) \
    .show()

    print("9.The total quantity of products sold in each city")
    df.groupBy("City") \
    .agg(sum("Quantity").alias("Total_Quantity")) \
    .orderBy(desc("Total_Quantity")) \
    .show()

    print("10.Customer segment has the highest profit margin")
    df.groupBy("Segment") \
    .agg((sum("Profit") / sum("Sales")).alias("Profit_Margin")) \
    .orderBy(desc("Profit_Margin")) \
    .show()