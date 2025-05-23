

def csv_reader(spark,filepath,schema,header=True, inferSchema=True, delimiter=','):
    df=spark.read.format('csv').option("header",header) \
    .schema(schema) \
    .option("delimiter",delimiter) \
    .load(filepath)
    return df