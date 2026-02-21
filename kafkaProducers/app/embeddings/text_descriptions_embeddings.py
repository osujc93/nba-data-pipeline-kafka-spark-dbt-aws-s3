from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit
from pyspark.sql.types import StructType, StructField, StringType
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct
import logging
import torch
from transformers import AutoTokenizer, AutoModel

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='app.log', filemode='w')
logger = logging.getLogger(__name__)

def create_spark_connection():
    try:
        spark = SparkSession.builder \
            .appName('TextDescriptionsEmbeddings') \
            .config('spark.jars.packages', "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,org.apache.iceberg:iceberg-spark-runtime-3.3_2.13:1.5.2") \
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
    return df.withColumn("descriptive_text", concat_ws(". ",
        lit("Game PK"), col("gamePk"),
        lit("Game Date"), col("gameDate"),
        lit("Headline"), col("headline"),
        lit("Duration"), col("duration"),
        lit("Title"), col("title"),
        lit("Description"), col("description"),
        lit("Slug"), col("slug"),
        lit("Blurb"), col("blurb"),
        lit("Date"), col("date"),
        lit("Content Date"), col("contentDate")
    ))

def generate_and_store_embeddings(df):
    model_name = "sentence-transformers/all-MiniLM-L6-v2"
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModel.from_pretrained(model_name)

    qdrant_client = QdrantClient(host="localhost", port=6333)

    qdrant_client.recreate_collection(
        collection_name="text_descriptions",
        vector_size=384,  # Embeddings size for MiniLM-L6
        distance="Cosine"
    )

    for row in df.collect():
        inputs = tokenizer(row.descriptive_text, return_tensors='pt', truncation=True, padding=True)
        with torch.no_grad():
            embeddings = model(**inputs).last_hidden_state.mean(dim=1).numpy()

        point = PointStruct(
            id=row.gamePk,
            vector=embeddings.tolist()[0],
            payload=row.asDict()
        )
        qdrant_client.upsert(collection_name="text_descriptions", points=[point])
        logger.info(f"Stored embedding for row ID: {row.gamePk} in Qdrant.")

def main():
    try:
        spark = create_spark_connection()
        descriptions_df = spark.read \
            .format("iceberg") \
            .load("hdfs://namenode:8020/warehouse/mlb_db/text_descriptions")

        transformed_df = transform_to_descriptive_text(descriptions_df)
        generate_and_store_embeddings(transformed_df)
        
        spark.stop()
        logger.info("Main function executed successfully.")
    except Exception as e:
        logger.error(f"Error in main function: {e}", exc_info=True)

if __name__ == "__main__":
    main()
