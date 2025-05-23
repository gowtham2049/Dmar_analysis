from pyspark.sql.functions import col
from functools import reduce


def cleaning(df):
    has_nulls = df.filter(
    reduce(lambda a, b: a | b, [col(c).isNull() for c in df.columns])
    ).count() > 0

    if has_nulls:
        return df.dropna()
    else:
        return df

def join(customer,products,sales):
    joined=sales.join(products, on="Product_ID", how="inner") \
            .join(customer, on="Customer_ID", how="inner")
    return joined