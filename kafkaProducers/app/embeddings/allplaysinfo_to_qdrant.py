import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit
from qdrant_client import QdrantClient
from qdrant_client.http.models import VectorParams
from transformers import AutoTokenizer, AutoModel
import torch
import concurrent.futures

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='app.log', filemode='w')
logger = logging.getLogger(__name__)

def create_spark_connection():
    try:
        spark = SparkSession.builder \
            .appName('AllPlaysEmbeddings') \
            .config('spark.jars.packages', "org.apache.iceberg:iceberg-spark-runtime-3.3_2.13:0.12.0") \
            .config('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkCatalog') \
            .config('spark.sql.catalog.spark_catalog.type', 'hadoop') \
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
            "allplays_info": spark.table("spark_catalog.mlb_db.allplays_info"),
            "allplays_aggregates": spark.table("spark_catalog.mlb_db.allplays_aggregates"),
            "season_totals": spark.table("spark_catalog.mlb_db.season_totals"),
            "season_averages": spark.table("spark_catalog.mlb_db.season_averages"),
            "future_allplays": spark.table("spark_catalog.mlb_db.future_allplays"),
            "player_bios": spark.table("spark_catalog.mlb_db.player_bios"),
            "historical_game_contexts": spark.table("spark_catalog.mlb_db.historical_game_contexts"),
            "advanced_metrics": spark.table("spark_catalog.mlb_db.advanced_metrics")
        }
        logger.info("Dataframes loaded successfully from Iceberg tables.")
        return dataframes
    except Exception as e:
        logger.error(f"Error loading dataframes: {e}", exc_info=True)
        raise

def generate_text_embeddings(df, columns):
    try:
        tokenizer = AutoTokenizer.from_pretrained("sentence-transformers/all-MiniLM-L6-v2")
        model = AutoModel.from_pretrained("sentence-transformers/all-MiniLM-L6-v2")

        text_representation = concat(*[col(column).cast("string").alias(column) for column in columns])
        df = df.withColumn("text_representation", text_representation)

        text_list = df.select("text_representation").rdd.map(lambda row: row.text_representation).collect()
        inputs = tokenizer(text_list, return_tensors='pt', padding=True, truncation=True)
        with torch.no_grad():
            embeddings = model(**inputs).last_hidden_state[:, 0, :].cpu().numpy()
        
        logger.info("Text embeddings generated successfully.")
        return embeddings, text_list
    except Exception as e:
        logger.error(f"Error generating text embeddings: {e}", exc_info=True)
        raise

def send_to_qdrant(embeddings, texts, collection_name):
    try:
        client = QdrantClient(url="http://localhost:6333")
        client.upload_collection(
            collection_name=collection_name,
            vectors=embeddings.tolist(),
            payload=[{"text": text} for text in texts],
            vector_params=VectorParams(size=embeddings.shape[1], distance="Cosine")
        )
        logger.info("Embeddings and texts uploaded to Qdrant successfully.")
    except Exception as e:
        logger.error(f"Error sending embeddings to Qdrant: {e}", exc_info=True)
        raise

def main():
    try:
        spark = create_spark_connection()
        
        # Define the collection name
        collection_name = "allplays_info"

        dataframes = load_dataframes(spark)
        table_columns = {
            "allplays_info": ["result_rbi", "result_awayScore", "result_homeScore", "about_captivatingIndex", "count_balls", "count_strikes", "count_outs"],
            "allplays_aggregates": ["total_rbi", "total_awayScore", "total_homeScore", "avg_captivatingIndex", "avg_balls", "avg_strikes", "avg_outs"],
            "season_totals": ["total_rbi", "total_awayScore", "total_homeScore", "total_pitches", "total_actions", "total_runners", "total_playEvents"],
            "season_averages": ["avg_captivatingIndex", "avg_balls", "avg_strikes", "avg_outs", "avg_pitches", "avg_actions", "avg_runners", "avg_playEvents"],
            "future_allplays": ["result_rbi", "result_awayScore", "result_homeScore", "about_captivatingIndex", "count_balls", "count_strikes", "count_outs"],
            "player_bios": ["player_name", "player_position", "player_biography"],
            "historical_game_contexts": ["game_date", "home_team", "away_team", "historical_context"],
            "advanced_metrics": ["metric_name", "metric_value"]
        }
        
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for table_name, columns in table_columns.items():
                df = dataframes[table_name]
                futures.append(executor.submit(generate_text_embeddings, df, columns))

            for future in concurrent.futures.as_completed(futures):
                embeddings, texts = future.result()
                send_to_qdrant(embeddings, texts, collection_name)
        
        spark.stop()
        logger.info("Main function executed successfully.")
    except Exception as e:
        logger.error(f"Error in main function: {e}", exc_info=True)

if __name__ == "__main__":
    main()
