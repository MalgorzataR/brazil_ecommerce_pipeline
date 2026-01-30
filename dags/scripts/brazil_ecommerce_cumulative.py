"""

"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main(project_id, dataset_id, output_table, temp_bucket):
    """
    Main function to transform Brazil ecommerce data.

    Reads source tables from BigQuery, performs joins and transformations
    to create cumulative table. Writes the result to a new BigQuery table.

    :param project_id: Google Cloud Project ID.
    :param dataset_id: BigQuery Dataset ID.
    :param output_table: Name of the output table.
    :param temp_bucket: Temporary GCS bucket for Spark BigQuery connector.
    """

    spark = (SparkSession.builder
            .appName("BigQueryConnectorTest")
            .config("temporaryGcsBucket", temp_bucket)
            .getOrCreate()
    )

    bq_tables = ["category", "order_items", "order_payments", "orders", "products"]
    brazil_dataframes = {}

    for table in bq_tables:
        df = (spark.read.format('bigquery')
                .option('table', f'{project_id}.{dataset_id}.{table}')
                .load())
        brazil_dataframes[table] = df

    payments = brazil_dataframes['order_payments']
    products = brazil_dataframes['products']
    category = brazil_dataframes['category']
    items = (
        brazil_dataframes["order_items"]
            .withColumn(
                "total_price",
                F.col("price") + F.col("freight_value"))
    )
    orders = (
        brazil_dataframes["orders"]
            .withColumn(
                "order_purchase_date",
                F.to_date(F.col("order_purchase_timestamp"))
            )
            .withColumn(
                "delivery_time_in_days",
                F.when(
                    F.col("order_delivered_customer_date").isNotNull(),
                    F.datediff(
                        F.to_date(F.col("order_delivered_customer_date")),
                        F.to_date(F.col("order_purchase_timestamp"))
                    )
                ).otherwise(None)
            )
            .withColumn(
                "week_end_date",
                F.date_add(F.col("order_purchase_timestamp"), 6 - F.dayofweek("order_purchase_timestamp"))
            )
            .withColumn(
                "month_end_date",
                F.last_day("order_purchase_timestamp")
            )
            .filter(F.col("order_status") == "delivered")
        .select(
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_delivered_customer_date",
            "order_purchase_date",
            "delivery_time_in_days",
            "week_end_date",
            "month_end_date"
        )
    )

    products = products.join(category.hint("broadcast"), "product_category_name")
    products = products.select(
        "product_id",
        "product_category_name",
        "product_category_name_english")

    orders_with_items = orders.join(items, "order_id").join(products, "product_id").join(payments, "order_id")
    orders_with_items = orders_with_items.select(
        'order_id',
        'product_id',
        'customer_id',
        'order_status',
        'order_purchase_timestamp',
        'order_delivered_customer_date',
        'order_purchase_date',
        'delivery_time_in_days',
        'week_end_date',
        'month_end_date',
        'order_item_id',
        'seller_id',
        'price',
        'freight_value',
        'total_price',
        F.col("product_category_name_english").alias("product_category_name"),
        'payment_sequential',
        'payment_type',
        'payment_value'
    )

    output = f"{project_id}.{dataset_id}.{output_table}"
    orders_with_items.write.format('bigquery').option('table', output).mode("overwrite").save()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_id", required=True)
    parser.add_argument("--dataset_id", required=True)
    parser.add_argument("--output_table", required=True)
    parser.add_argument("--temp_bucket", required=True)
    args = parser.parse_args()

    main(args.project_id, args.dataset_id, args.output_table, args.temp_bucket)
