import argparse

from pyspark.sql import SparkSession


def main(src_table, project_id, dataset_id, output_table, temp_bucket):
    """
    Main function to analyze Brazil ecommerce data.

    Reads a source table from BigQuery, joins dataset with backbone and performs 
    aggregations on few levels of granularity (weekly and monthly, category, seller_id).
    The final results are written to BigQuery.

    :param src_table: Source table name in BigQuery.
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

    df = (spark.read.format('bigquery')
                .option('table', f'{project_id}.{dataset_id}.{src_table}')
                .load())
    df.createOrReplaceTempView("brazil_cumulative")

    brazil_cumulative = spark.sql(
        """
        SELECT
            week_end_date AS observation_week,
            product_category_name AS category,
            payment_type,
            seller_id,
            customer_id,
            order_id,
            delivery_time_in_days,
            order_item_id,
            price,
            freight_value,
            total_price
        FROM brazil_cumulative
        """
    )

    brazil_cumulative.cache()
    brazil_cumulative.createOrReplaceTempView("brazil_cumulative")
        
    spark.sql(
        """
        WITH per_pair AS (
            SELECT
                category,
                MIN(observation_week) AS start_week,
                MAX(observation_week) AS end_week
            FROM brazil_cumulative
            GROUP BY category
        )
        SELECT
            category,
            EXPLODE(
            SEQUENCE(start_week, end_week, INTERVAL 1 WEEK)
            ) AS observation_week
        FROM per_pair
        """
    ).createOrReplaceTempView("backbone")

    spark.sql(
        """
        SELECT /*+ BROADCAST(bc) */ DISTINCT
            b.observation_week,
            LAST_DAY(b.observation_week) AS observation_month,
            b.category,
            IF(bc.observation_week IS NULL, True, False) AS is_interpolated,
            bc.seller_id,
            bc.customer_id,
            bc.delivery_time_in_days,
            bc.order_id,
            bc.order_item_id,
            bc.freight_value,
            bc.total_price
        FROM backbone AS b
        LEFT JOIN brazil_cumulative AS bc
            ON b.observation_week = bc.observation_week
            AND b.category = bc.category
        """
    ).createOrReplaceTempView("join_backbone")

    spark.sql(
        """
        SELECT 
            exploded_data.frequency AS frequency,
            exploded_data.period_end_date AS period_end_date,
            jb.category,
            jb.is_interpolated,
            jb.seller_id,
            jb.customer_id,
            jb.delivery_time_in_days,
            jb.order_id,
            jb.order_item_id,
            jb.freight_value,
            jb.total_price
        FROM join_backbone jb
        LATERAL VIEW EXPLODE(
            ARRAY(
                NAMED_STRUCT("frequency", 'MONTHLY', "period_end_date", observation_month),
                NAMED_STRUCT("frequency", 'WEEKLY', "period_end_date", observation_week)
            )
        ) AS exploded_data
        """
    ).createOrReplaceTempView("exploded_results")

    spark.sql(
        """
        SELECT 
            period_end_date,
            frequency,
            category,
            seller,
            MAX(number_of_customers) AS number_of_customers,
            MAX(average_delivery_time_in_days) AS average_delivery_time_in_days,
            MAX(number_of_orders) AS number_of_orders,
            SUM(item_inventory) AS item_inventory,
            SUM(total_transport_price) AS total_transport_price,
            SUM(total_value) AS total_value
        FROM (
            SELECT DISTINCT
                period_end_date,
                frequency,
                COALESCE(category, 'All') AS category,
                COALESCE(seller_id, 'All') AS seller,
                COUNT(customer_id) AS number_of_customers,
                ROUND(AVG(delivery_time_in_days), 2) AS average_delivery_time_in_days,
                COUNT(order_id) AS number_of_orders,
                SUM(order_item_id) AS item_inventory,
                ROUND(SUM(freight_value), 2) AS total_transport_price,
                ROUND(SUM(total_price), 2) AS total_value
            FROM exploded_results
            GROUP BY GROUPING SETS (
                (period_end_date, frequency),
                (period_end_date, frequency, category),
                (period_end_date, frequency, category, seller_id)
            ) 
        ) a
        GROUP BY period_end_date, frequency, category, seller
        """
    ).createOrReplaceTempView("final_results")


    output = f"{project_id}.{dataset_id}.{output_table}"
    spark.table("final_results") \
        .write \
        .format("bigquery") \
        .option("table", output) \
        .mode("overwrite") \
        .save()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--src_table", required=True)
    parser.add_argument("--project_id", required=True)
    parser.add_argument("--dataset_id", required=True)
    parser.add_argument("--output_table", required=True)
    parser.add_argument("--temp_bucket", required=True)
    args = parser.parse_args()

    main(args.src_table, args.project_id, args.dataset_id, args.output_table, args.temp_bucket)
