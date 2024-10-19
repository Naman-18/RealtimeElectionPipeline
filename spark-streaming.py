import pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.functions import sum as _sum, count
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from utils import load_config

# Aggregation functions
def aggregate_votes_per_candidate(df: DataFrame) -> DataFrame:
    return df.groupBy("candidate_id", "candidate_name", "party_affiliation", "photo_url") \
             .agg(_sum("vote").alias("total_votes"))

def aggregate_votes_per_party(df: DataFrame) -> DataFrame:
    return df.groupBy("party_affiliation") \
             .agg(_sum("vote").alias("total_votes"))

def aggregate_turnout_by_location(df: DataFrame) -> DataFrame:
    return df.groupBy("address_state") \
             .agg(count("*").alias("total_votes"))

def aggregate_turnout_by_gender(df: DataFrame) -> DataFrame:
    return df.groupBy("gender") \
             .agg(count("*").alias("total_votes"))

# Kafka writer function
def write_to_kafka(df: DataFrame, topic: str, checkpoint_dir: str, config: dict):
    return df.selectExpr("to_json(struct(*)) AS value") \
             .writeStream \
             .format("kafka") \
             .option("kafka.bootstrap.servers", "localhost:9092") \
             .option("topic", topic) \
             .option("checkpointLocation", checkpoint_dir) \
             .outputMode("update") \
             .start()

# Schema definition
def get_vote_schema() -> StructType:
    return StructType([
        StructField("voter_id", StringType(), True),
        StructField("candidate_id", StringType(), True),
        StructField("voting_time", TimestampType(), True),
        StructField("voter_name", StringType(), True),
        StructField("party_affiliation", StringType(), True),
        StructField("biography", StringType(), True),
        StructField("campaign_platform", StringType(), True),
        StructField("photo_url", StringType(), True),
        StructField("candidate_name", StringType(), True),
        StructField("date_of_birth", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("nationality", StringType(), True),
        StructField("registration_number", StringType(), True),
        StructField("address_street", StringType(), True),
        StructField("address_city", StringType(), True),
        StructField("address_state", StringType(), True),
        StructField("address_country", StringType(), True),
        StructField("address_postcode", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("cell_number", StringType(), True),
        StructField("picture", StringType(), True),
        StructField("registered_age", IntegerType(), True),
        StructField("vote", IntegerType(), True)
    ])

# Ingest data from Kafka
def ingest_data_from_kafka(spark: SparkSession, schema: StructType, config: dict) -> DataFrame:
    return (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "votes_topic")
            .option("startingOffsets", "earliest")
            .load()
            .selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), schema).alias("data"))
            .select("data.*")
    )

# Enrich data (add casting and watermarking)
def enrich_data(df: DataFrame) -> DataFrame:
    return (df.withColumn("voting_time", col("voting_time").cast(TimestampType()))
              .withColumn('vote', col('vote').cast(IntegerType()))
              .withWatermark("voting_time", "1 minute"))

# Main function to run the pipeline
def run_pipeline():
    config = load_config('config.json')

    # Initialize SparkSession
    spark = (SparkSession.builder
             .appName("RealtimeElectionPipeline")
             .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.scala-lang:scala-library:2.12.18')
             .config('spark.jars', config['base_dir'] + 'postgresql-42.7.4.jar')
             .getOrCreate())

    # Define schema
    vote_schema = get_vote_schema()

    # Ingest and enrich data
    votes_df = ingest_data_from_kafka(spark, vote_schema, config)
    enriched_votes_df = enrich_data(votes_df)

    # Aggregates
    votes_per_candidate = aggregate_votes_per_candidate(enriched_votes_df)
    votes_per_party = aggregate_votes_per_party(enriched_votes_df)
    turnout_by_location = aggregate_turnout_by_location(enriched_votes_df)
    turnout_by_gender = aggregate_turnout_by_gender(enriched_votes_df)

    # Write to Kafka
    aggregated_votes_per_candidate = write_to_kafka(votes_per_candidate, "aggregated_votes_per_candidate", config['base_dir'] + "checkpoints/aggregated_votes_per_candidate", config)
    aggregated_votes_per_party = write_to_kafka(votes_per_party, "aggregated_votes_per_party", config['base_dir'] + "checkpoints/aggregated_votes_per_party", config)
    aggregated_turnout_by_location = write_to_kafka(turnout_by_location, "aggregated_turnout_by_location", config['base_dir'] + "checkpoints/aggregated_turnout_by_location", config)
    aggregated_turnout_by_gender = write_to_kafka(turnout_by_gender, "aggregated_turnout_by_gender", config['base_dir'] + "checkpoints/aggregated_turnout_by_gender", config)

    # Await termination for the streaming queries
    aggregated_votes_per_candidate.awaitTermination()
    aggregated_votes_per_party.awaitTermination()
    aggregated_turnout_by_location.awaitTermination()
    aggregated_turnout_by_gender.awaitTermination()

if __name__ == '__main__':
    run_pipeline()
