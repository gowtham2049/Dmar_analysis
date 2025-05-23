from Scripts.spark import spark_creator
from Scripts.load import csv_reader
from Scripts.schema import sales_schema,customer_schema,product_schema
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType,DoubleType
from Scripts.transformation import cleaning,join
from Scripts.analysis import analysis


def main():
    spark=spark_creator()
    product_df=csv_reader(spark,"dataset\Product.csv",product_schema)
    customer_df=csv_reader(spark,"dataset\Customer.csv",customer_schema)
    sales_df=csv_reader(spark,"dataset\Sales.csv",sales_schema)
    print("loaded")
    cleaned_product=cleaning(product_df).cache()
    cleaned_customer=cleaning(customer_df).cache()
    cleaned_sales=cleaning(sales_df).cache()
    cleaned_product.count()
    cleaned_customer.count()
    cleaned_sales.count()
    print("cleaned")

    final_df=join(cleaned_customer,cleaned_product,cleaned_sales)
    print("joined")
    analysis(final_df)
    input("enter to stop")

    spark.stop()



if __name__=="__main__":
    main()



