#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, hour, dayofweek, countDistinct, mean, sum as spark_sum
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.linalg import Vectors
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import logging

# Configure logging
logging.basicConfig(
    filename='data_processing.log',
    level=logging.DEBUG,
    format='%(asctime)s:%(levelname)s:%(message)s',
    filemode='w'
)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(message)s'))
logging.getLogger().addHandler(console_handler)

def log_start_end(func):
    """Log the start and end of functions."""
    def wrapper(*args, **kwargs):
        logging.info(f"Started {func.__name__}")
        result = func(*args, **kwargs)
        logging.info(f"Finished {func.__name__}")
        return result
    return wrapper

@log_start_end
def load_data_from_gcs(spark, bucket_name, file_path, schema):
    """Load the dataset from GCS with a defined schema."""
    try:
        logging.debug(f"Loading data from gs://{bucket_name}/{file_path}")
        data = spark.read.format("csv").option("header", "true").schema(schema).load(f"gs://{bucket_name}/{file_path}")
        logging.info(f"Loaded data from gs://{bucket_name}/{file_path} successfully.")
        return data
    except Exception as e:
        logging.error(f"Error loading data from gs://{bucket_name}/{file_path}: {e}")
        raise

@log_start_end
def transform_data(data):
    """Transform dataset."""
    try:
        logging.debug("Starting data transformation.")
        
        # Convert event_time to timestamp and extract additional time features
        data = data.withColumn('event_time', col('event_time').cast('timestamp'))
        data = data.withColumn('day_of_week', date_format(col('event_time'), 'EEEE'))
        data = data.withColumn('hour_of_day', hour(col('event_time')))
        data = data.withColumn('day_of_week_num', dayofweek(col('event_time')))
        
        # Fill missing category_code with 'unknown'
        data = data.fillna({'category_code': 'unknown'})
        
        # Remove outliers in price
        quantiles = data.approxQuantile("price", [0.01, 0.99], 0.0)
        data = data.filter((col("price") >= quantiles[0]) & (col("price") <= quantiles[1]))
        
        # Scale/normalize the price column
        price_vec = data.select(col("price")).rdd.map(lambda x: [Vectors.dense(x[0])])
        df_price_vec = spark.createDataFrame(price_vec, ["features"])
        scaler = MinMaxScaler(inputCol="features", outputCol="scaled_features")
        scaler_model = scaler.fit(df_price_vec)
        scaled_data = scaler_model.transform(df_price_vec)
        scaled_prices = scaled_data.select(col("scaled_features")[0].alias("scaled_price"))
        data = data.withColumn("scaled_price", scaled_prices["scaled_price"])

        # Derive new features like total sales per user and average price per category
        total_sales_per_user = data.groupBy("user_id").agg(spark_sum("price").alias("total_sales"))
        avg_price_per_category = data.groupBy("category_code").agg(mean("price").alias("avg_price"))

        # Aggregating data to summarize information
        aggregated_data = data.groupBy("event_type", "brand", "category_code").agg(
            spark_sum("price").alias("total_sales"),
            mean("price").alias("avg_price"),
            countDistinct("user_id").alias("unique_users")
        )

        # Grouping data by user sessions to analyze user behavior within a session
        session_data = data.groupBy("user_session").agg(
            countDistinct("event_type").alias("unique_event_types"),
            spark_sum("price").alias("total_session_sales"),
            countDistinct("product_id").alias("unique_products_viewed")
        )

        logging.info("Data transformation completed successfully.")
        return data, total_sales_per_user, avg_price_per_category, aggregated_data, session_data
    except Exception as e:
        logging.error(f"Error transforming data: {e}")
        raise

@log_start_end
def save_data_to_bigquery(data, project_id, dataset_name, table_name):
    """Save the transformed data to BigQuery."""
    try:
        logging.debug(f"Saving data to BigQuery dataset {dataset_name}, table {table_name}.")
        data.write \
            .format('bigquery') \
            .option('temporaryGcsBucket', 'gs://ecommercee/tmp/') \
            .option('table', f"{project_id}:{dataset_name}.{table_name}") \
            .save()
        logging.info(f"Saved transformed data to BigQuery table {table_name} successfully.")
    except Exception as e:
        logging.error(f"Error saving data to BigQuery: {e}")
        raise

def main():
    bucket_name = 'ecommercee'
    file_paths = ['2019-Oct.csv', '2019-Nov.csv', '2019-Dec.csv', '2020-Jan.csv', '2020-Feb.csv']
    project_id = 'icezy-423617'
    dataset_name = 'Ecommerce'
    table_name = 'sales_transaction'

    spark = SparkSession.builder \
        .appName("GCS_to_BigQuery") \
        .getOrCreate()

    # Define the schema
    schema = StructType([
        StructField("event_time", TimestampType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("category_id", StringType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("user_id", StringType(), True),
        StructField("user_session", StringType(), True)
    ])

    # Load and merge all datasets
    all_data = None
    for file_path in file_paths:
        data = load_data_from_gcs(spark, bucket_name, file_path, schema)
        if all_data is None:
            all_data = data
        else:
            all_data = all_data.union(data)

    # Transform and save the merged dataset
    transformed_data, total_sales_per_user, avg_price_per_category, aggregated_data, session_data = transform_data(all_data)
    save_data_to_bigquery(transformed_data, project_id, dataset_name, table_name)
    save_data_to_bigquery(total_sales_per_user, project_id, dataset_name, "total_sales_per_user")
    save_data_to_bigquery(avg_price_per_category, project_id, dataset_name, "avg_price_per_category")
    save_data_to_bigquery(aggregated_data, project_id, dataset_name, "aggregated_data")
    save_data_to_bigquery(session_data, project_id, dataset_name, "session_data")

    spark.stop()

if __name__ == "__main__":
    logging.info("Data processing started.")
    try:
        main()
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    logging.info("Data processing finished.")

