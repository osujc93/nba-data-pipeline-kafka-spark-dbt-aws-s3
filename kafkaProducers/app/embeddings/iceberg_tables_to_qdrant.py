import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType
from transformers import pipeline
import qdrant_client
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='embedding_app.log', filemode='w')
logger = logging.getLogger(__name__)

def create_spark_connection():
    """
    Creates and returns a Spark session configured to use Iceberg for table management.
    """
    try:
        spark = SparkSession.builder \
            .appName('TextEmbeddingsAndDescriptions') \
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

def create_text_descriptions(df):
    """
    Creates text descriptions from the numerical, boolean, and string values in the DataFrame.
    
    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: A DataFrame with a new column 'description' containing the text descriptions.
    """
    try:
        def value_to_text(value):
            if value is None:
                return "null"
            return str(value)

        text_df = df.select([value_to_text(col(c)).alias(c) for c in df.columns])
        description_df = text_df.withColumn(
            "description",
            lit(" ").join([col(c).cast(StringType()) for c in text_df.columns])
        )
        logger.info("Text descriptions created successfully.")
        return description_df
    except Exception as e:
        logger.error(f"Error creating text descriptions: {e}", exc_info=True)
        raise

def generate_embeddings(descriptions):
    """
    Generates embeddings for the provided text descriptions using a pre-trained transformer model.
    
    Args:
        descriptions (list): A list of text descriptions.

    Returns:
        list: A list of embeddings.
    """
    try:
        embedder = pipeline('feature-extraction', model='sentence-transformers/all-MiniLM-L6-v2')
        embeddings = [embedder(description)[0][0] for description in descriptions]
        logger.info("Embeddings generated successfully.")
        return embeddings
    except Exception as e:
        logger.error(f"Error generating embeddings: {e}", exc_info=True)
        raise

def send_embeddings_to_qdrant(embeddings):
    """
    Sends the generated embeddings to Qdrant vector DB.
    
    Args:
        embeddings (list): A list of embeddings.
    """
    try:
        client = qdrant_client.QdrantClient(host='localhost', port=6333)
        vectors = np.array(embeddings).tolist()
        points = [{'id': i, 'vector': vec} for i, vec in enumerate(vectors)]
        client.upsert(collection_name='mlb_embeddings', points=points)
        logger.info("Embeddings sent to Qdrant successfully.")
    except Exception as e:
        logger.error(f"Error sending embeddings to Qdrant: {e}", exc_info=True)
        raise

def process_table(spark, table_name):
    """
    Processes a specific table: reads the table, creates text descriptions, generates embeddings, and sends them to Qdrant.
    
    Args:
        spark (SparkSession): The Spark session object.
        table_name (str): The name of the table to process.
    """
    try:
        df = spark.table(f"spark_catalog.default.{table_name}")
        desc_df = create_text_descriptions(df)
        embeddings = generate_embeddings(desc_df.select("description").rdd.flatMap(lambda x: x).collect())
        send_embeddings_to_qdrant(embeddings)
        logger.info(f"Processed table {table_name} successfully.")
    except Exception as e:
        logger.error(f"Error processing table {table_name}: {e}", exc_info=True)
        raise

def main():
    """
    The main function to execute the text description and embedding generation pipeline for all tables in Iceberg.
    """
    try:
        spark = create_spark_connection()

        # List all tables in the default Iceberg catalog
        tables = spark.sql("SHOW TABLES IN spark_catalog.default").collect()
        table_names = [table.tableName for table in tables]
        
        # Process each table
        for table_name in table_names:
            process_table(spark, table_name)

        # Stop Spark session
        spark.stop()
        
        logging.info("Text embedding and description pipeline executed successfully for all tables.")
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)

if __name__ == "__main__":
    main()
