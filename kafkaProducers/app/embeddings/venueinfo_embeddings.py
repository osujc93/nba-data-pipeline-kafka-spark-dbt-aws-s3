import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from qdrant_client import QdrantClient
from transformers import AutoTokenizer, AutoModel
import torch

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='app.log', filemode='w')
logger = logging.getLogger(__name__)

def create_spark_connection():
    try:
        spark = SparkSession.builder \
            .appName('VenueInfoEmbeddings') \
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

def load_dataframes(spark):
    try:
        dataframes = {
            'dim_venue': spark.table('spark_catalog.default.dim_venue'),
            'fact_game_info': spark.table('spark_catalog.default.fact_game_info')
        }
        logger.info("Dataframes loaded successfully.")
        return dataframes
    except Exception as e:
        logger.error(f"Error loading dataframes: {e}", exc_info=True)
        raise

def create_embeddings(texts, model_name='sentence-transformers/all-MiniLM-L6-v2'):
    try:
        tokenizer = AutoTokenizer.from_pretrained(model_name)
        model = AutoModel.from_pretrained(model_name)
        inputs = tokenizer(texts, return_tensors='pt', padding=True, truncation=True)
        with torch.no_grad():
            outputs = model(**inputs)
        embeddings = outputs.last_hidden_state.mean(dim=1)
        return embeddings.detach().numpy()
    except Exception as e:
        logger.error(f"Error creating embeddings: {e}", exc_info=True)
        raise

def convert_to_text(row):
    try:
        text = f"Venue ID: {row['venue_id']}, Venue Name: {row['venue_name']}, Active: {row['venue_active']}, " \
               f"Season: {row['venue_season']}, Address: {row['venue_location_address1']}, City: {row['venue_location_city']}, " \
               f"State: {row['venue_location_state']}, Postal Code: {row['venue_location_postalCode']}, " \
               f"Country: {row['venue_location_country']}, Time Zone: {row['venue_timeZone_id']}, " \
               f"Capacity: {row['venue_fieldInfo_capacity']}, Turf Type: {row['venue_fieldInfo_turfType']}, " \
               f"Roof Type: {row['venue_fieldInfo_roofType']}, Game Date: {row['game_date']}, " \
               f"Game Time: {row['game_time']}, Game ID: {row['game_id']}"
        return text
    except Exception as e:
        logger.error(f"Error converting row to text: {e}", exc_info=True)
        raise

def process_and_send_to_qdrant(dataframes):
    try:
        qdrant_client = QdrantClient(url="http://localhost:6333")
        for df_name, df in dataframes.items():
            texts = df.rdd.map(lambda row: convert_to_text(row.asDict())).collect()
            embeddings = create_embeddings(texts)
            for idx, text in enumerate(texts):
                qdrant_client.upsert(
                    collection_name='venue_info',
                    points=[{
                        'id': f"{df_name}_{idx}",
                        'payload': {'text': text},
                        'vector': embeddings[idx].tolist()
                    }]
                )
        logger.info("Data processed and sent to Qdrant successfully.")
    except Exception as e:
        logger.error(f"Error processing and sending data to Qdrant: {e}", exc_info=True)
        raise

def main():
    try:
        spark = create_spark_connection()
        dataframes = load_dataframes(spark)
        process_and_send_to_qdrant(dataframes)
        spark.stop()
        logging.info("Rich text embeddings and descriptions processed successfully.")
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)

if __name__ == "__main__":
    main()
