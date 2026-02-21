from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws
import logging
import os
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='embedding.log', filemode='w')
logger = logging.getLogger(__name__)

def create_spark_connection():
    try:
        azure_account_name = os.getenv('AZURE_ACCOUNT_NAME')
        azure_account_key = os.getenv('AZURE_ACCOUNT_KEY')
        azure_container = os.getenv('AZURE_CONTAINER')
        
        spark = SparkSession.builder \
            .appName('BoxscoreEmbeddings') \
            .config("spark.jars.packages", 
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                    "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.5.2,"
                    "org.apache.hadoop:hadoop-azure:3.4.0,"
                    "org.apache.hadoop:hadoop-azure-datalake:3.4.0") \
            .config("spark.hadoop.fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem") \
            .config(f"spark.hadoop.fs.azure.account.key.{azure_account_name}.blob.core.windows.net", azure_account_key) \
            .config("spark.sql.warehouse.dir", f"abfss://{azure_container}@{azure_account_name}.dfs.core.windows.net/warehouse") \
            .enableHiveSupport() \
            .getOrCreate()
        
        spark.sql("CREATE DATABASE IF NOT EXISTS mlb_db")
        
        logger.info("Spark connection created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark connection: {e}", exc_info=True)
        raise

def read_tables(spark):
    try:
        # Existing tables
        tables = [
            "future_dim_home_teamStats", "future_dim_away_teamStats", "future_dim_home_players", "future_dim_away_players",
            "dim_home_teamStats", "dim_away_teamStats", "aggregated_home_team_season_totals", "aggregated_away_team_season_totals",
            "aggregated_home_players_season_totals", "aggregated_away_players_season_totals", "aggregated_home_team_season_averages",
            "aggregated_away_team_season_averages", "aggregated_home_players_season_averages", "aggregated_away_players_season_averages",
            "cumulative_home_team_season_totals", "cumulative_away_team_season_totals", "cumulative_home_players_season_totals",
            "cumulative_away_players_season_totals", "cumulative_home_team_season_averages", "cumulative_away_team_season_averages",
            "cumulative_home_players_season_averages", "cumulative_away_players_season_averages"
        ]
        
        dataframes = {}
        for table in tables:
            dataframes[table] = spark.read.format("iceberg").load(f"hdfs://namenode:8020/warehouse/mlb_db/{table}")
        
        logger.info("Tables read successfully.")
        return dataframes
    except Exception as e:
        logger.error(f"Error reading tables: {e}", exc_info=True)
        raise

def generate_text(df):
    try:
        text_df = df.select(
            concat_ws(" ", *[col(c).cast("string") for c in df.columns]).alias("text")
        )
        return text_df
    except Exception as e:
        logger.error(f"Error generating text: {e}", exc_info=True)
        raise

def send_to_qdrant(text_df, collection_name):
    try:
        model = SentenceTransformer('all-MiniLM-L6-v2')
        texts = [row.text for row in text_df.collect()]
        embeddings = model.encode(texts, convert_to_tensor=True)
        
        qdrant_client = QdrantClient(host="qdrant", port=6333)
        points = [{'id': str(idx), 'vector': emb} for idx, emb in enumerate(embeddings)]
        qdrant_client.upsert(collection_name=collection_name, points=points)
        
        logger.info(f"Text embeddings sent to Qdrant for collection: {collection_name}")
    except Exception as e:
        logger.error(f"Error sending text embeddings to Qdrant: {e}", exc_info=True)
        raise

def main():
    try:
        spark = create_spark_connection()
        dataframes = read_tables(spark)
        
        for table_name, df in dataframes.items():
            text_df = generate_text(df)
            send_to_qdrant(text_df, table_name)

        spark.stop()
        logger.info("Text embedding script executed successfully.")
    except Exception as e:
        logger.error(f"Error in main function: {e}", exc_info=True)

if __name__ == "__main__":
    main()
