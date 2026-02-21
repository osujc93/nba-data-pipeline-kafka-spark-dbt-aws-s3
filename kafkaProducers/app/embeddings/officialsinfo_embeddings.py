from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws
from pyspark.sql.types import IntegerType, BooleanType
import torch
from transformers import AutoTokenizer, AutoModel
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='embedding_consumer.log', filemode='w')
logger = logging.getLogger(__name__)

def create_spark_connection():
    try:
        spark = SparkSession.builder \
            .appName('OfficialsInfoEmbeddings') \
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

def convert_numerical_to_text(df):
    try:
        for column in df.columns:
            if isinstance(df.schema[column].dataType, IntegerType):
                df = df.withColumn(column, col(column).cast("string"))
            elif isinstance(df.schema[column].dataType, BooleanType):
                df = df.withColumn(column, col(column).cast("string"))
        return df
    except Exception as e:
        logger.error(f"Error in convert_numerical_to_text function: {e}", exc_info=True)
        raise

def generate_embeddings(df, model_name="sentence-transformers/all-MiniLM-L6-v2"):
    try:
        tokenizer = AutoTokenizer.from_pretrained(model_name)
        model = AutoModel.from_pretrained(model_name)

        def embed_text(text):
            inputs = tokenizer(text, return_tensors='pt', truncation=True, padding=True)
            outputs = model(**inputs)
            return outputs.last_hidden_state.mean(dim=1).detach().numpy().flatten()

        texts = df.rdd.map(lambda row: row[0]).collect()
        embeddings = [embed_text(text) for text in texts]
        return embeddings, texts
    except Exception as e:
        logger.error(f"Error in generate_embeddings function: {e}", exc_info=True)
        raise

def send_to_qdrant(embeddings, texts, collection_name):
    try:
        client = QdrantClient(host="localhost", port=6333)

        points = [
            PointStruct(id=index, vector=embedding.tolist(), payload={"text": text})
            for index, (embedding, text) in enumerate(zip(embeddings, texts))
        ]

        client.upsert(
            collection_name=collection_name,
            wait=True,
            points=points
        )

        logger.info(f"Data sent to Qdrant collection {collection_name} successfully.")
    except Exception as e:
        logger.error(f"Error in send_to_qdrant function: {e}", exc_info=True)
        raise

def process_and_store_embeddings(spark, table_name, collection_name):
    try:
        df = spark.read.format("iceberg").load(f"hdfs://namenode:8020/warehouse/mlb_db/{table_name}")
        df = convert_numerical_to_text(df)
        df = df.withColumn("combined_text", concat_ws(" ", *df.columns))
        texts_df = df.select("combined_text")
        embeddings, texts = generate_embeddings(texts_df)
        send_to_qdrant(embeddings, texts, collection_name)
    except Exception as e:
        logger.error(f"Error in process_and_store_embeddings function for table {table_name}: {e}", exc_info=True)
        raise

def main():
    try:
        spark = create_spark_connection()

        # Define the collection name
        collection_name = "officials_info"

        # List of tables to process
        tables = ["dim_officials", "fact_officials"]

        for table in tables:
            process_and_store_embeddings(spark, table, collection_name)

        spark.stop()
        logger.info("Main function executed successfully.")
    except Exception as e:
        logger.error(f"Error in main function: {e}", exc_info=True)

if __name__ == "__main__":
    main()
