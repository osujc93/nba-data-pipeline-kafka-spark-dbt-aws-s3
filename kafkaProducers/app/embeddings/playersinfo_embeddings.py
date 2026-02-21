from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit
import torch
from transformers import AutoTokenizer, AutoModel
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='consumer_app.log', filemode='w')
logger = logging.getLogger(__name__)

def create_spark_connection():
    try:
        spark = SparkSession.builder \
            .appName('PlayersInfoEmbeddings') \
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

def transform_to_descriptive_text(df, table_name):
    if table_name == "playersinfo":
        df = df.withColumn("descriptive_text", concat_ws(". ",
            lit("Player ID"), col("player_id"),
            lit("Player Name"), col("player_name"),
            lit("Team ID"), col("team_id"),
            lit("Team Name"), col("team_name"),
            lit("Position"), col("primary_position_name"),
            lit("Birth Date"), col("birth_date"),
            lit("Birth City"), col("birth_city"),
            lit("Birth Country"), col("birth_country"),
            lit("Height"), col("height"),
            lit("Weight"), col("weight"),
            lit("MLB Debut Date"), col("mlb_debut_date")
        ))
    return df

def generate_and_store_embeddings(df, table_name):
    model_name = "sentence-transformers/all-MiniLM-L6-v2"
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModel.from_pretrained(model_name)

    qdrant_client = QdrantClient(host="qdrant", port=6333)

    for row in df.collect():
        inputs = tokenizer(row.descriptive_text, return_tensors='pt', truncation=True, padding=True)
        with torch.no_grad():
            embeddings = model(**inputs).last_hidden_state.mean(dim=1).numpy()

        point = PointStruct(
            id=row.player_id,  # Use a unique ID for each point
            vector=embeddings.tolist()[0],
            payload={"descriptive_text": row.descriptive_text, "table": table_name}
        )
        qdrant_client.upsert(collection_name="players_info", points=[point])
        logger.info(f"Stored embedding for row ID: {row.player_id} from table {table_name} in Qdrant.")

def process_and_store_embeddings(spark, table_name):
    df = spark.read.format("iceberg").load(f"hdfs://namenode:8020/warehouse/mlb_db/{table_name}")
    df = transform_to_descriptive_text(df, table_name)
    generate_and_store_embeddings(df, table_name)

def main():
    try:
        spark = create_spark_connection()

        table_name = "playersinfo"
        process_and_store_embeddings(spark, table_name)
        
        spark.stop()
        logger.info("Main function executed successfully.")
    except Exception as e:
        logger.error(f"Error in main function: {e}", exc_info=True)

if __name__ == "__main__":
    main()
