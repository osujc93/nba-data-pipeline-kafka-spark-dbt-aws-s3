import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit
import torch
from transformers import AutoTokenizer, AutoModel
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='consumer_app.log', filemode='w')
logger = logging.getLogger(__name__)

def create_spark_connection():
    try:
        spark = SparkSession.builder \
            .appName('WeatherInfoEmbeddings') \
            .config('spark.jars.packages', "org.apache.iceberg:iceberg-spark-runtime-3.3_2.13:1.5.2") \
            .config('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkCatalog') \
            .config('spark.sql.catalog.spark_catalog.type', 'hive') \
            .config('spark.sql.catalog.spark_catalog.uri', 'thrift://hive-metastore:9083') \
            .config('spark.sql.catalog.spark_catalog.warehouse', 'hdfs://namenode:8020/warehouse') \
            .getOrCreate()
        
        logger.info("Spark connection created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark connection: {e}", exc_info=True)
        raise

def transform_to_descriptive_text(df):
    df = df.withColumn("descriptive_text", concat_ws(". ",
        lit("The weather condition is"), col("weather_condition"),
        lit("with a temperature of"), col("weather_temp"),
        lit("and wind speed of"), col("weather_wind"),
        lit("on"), col("game_date"),
        lit("at"), col("game_time"),
        lit("for game ID"), col("game_id")
    ))
    return df

def generate_and_store_embeddings(df):
    model_name = "sentence-transformers/all-MiniLM-L6-v2"
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModel.from_pretrained(model_name)

    qdrant_client = QdrantClient(host="localhost", port=6333)

    for row in df.collect():
        inputs = tokenizer(row.descriptive_text, return_tensors='pt', truncation=True, padding=True)
        with torch.no_grad():
            embeddings = model(**inputs).last_hidden_state.mean(dim=1).numpy()

        point = PointStruct(
            id=row.game_id,
            vector=embeddings.tolist()[0],
            payload={"descriptive_text": row.descriptive_text}
        )
        qdrant_client.upsert(collection_name="weather_info", points=[point])
        logger.info(f"Stored embedding for game ID: {row.game_id} in Qdrant.")

def main():
    try:
        spark = create_spark_connection()

        # Read from Iceberg table
        weather_info_df = spark.read.format("iceberg").load("hdfs://namenode:8020/warehouse/mlb_db/weather_info")

        # Transform to descriptive texts
        weather_info_df = transform_to_descriptive_text(weather_info_df)

        # Generate and store embeddings in Qdrant
        generate_and_store_embeddings(weather_info_df)
        
        spark.stop()
        logger.info("Main function executed successfully.")
    except Exception as e:
        logger.error(f"Error in main function: {e}", exc_info=True)

if __name__ == "__main__":
    main()
