from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType,DoubleType

sales_schema=StructType([StructField('Order_Line', IntegerType(), True), 
            StructField('Order_ID', StringType(), True), 
            StructField('Order_Date', DateType(), True), 
            StructField('Ship_Date', DateType(), True), 
            StructField('Ship_Mode', StringType(), True), 
            StructField('Customer_ID', StringType(), True), 
            StructField('Product_ID', StringType(), True), 
            StructField('Sales', DoubleType(), True), 
            StructField('Quantity', IntegerType(), True), 
            StructField('Discount', DoubleType(), True), 
            StructField('Profit', DoubleType(), True)])

customer_schema=StructType([StructField('Customer_ID', StringType(), True), 
            StructField('Customer_Name', StringType(), True), 
            StructField('Segment', StringType(), True), 
            StructField('Age', IntegerType(), True), 
            StructField('Country', StringType(), True), 
            StructField('City', StringType(), True),
            StructField('State', StringType(), True), 
            StructField('Postal_Code', IntegerType(), True), 
            StructField('Region', StringType(), True)])


product_schema=StructType([StructField('Product_ID', StringType(), True), 
            StructField('Category', StringType(), True), 
            StructField('Sub_Category', StringType(), True), 
            StructField('Product_Name', StringType(), True)])
