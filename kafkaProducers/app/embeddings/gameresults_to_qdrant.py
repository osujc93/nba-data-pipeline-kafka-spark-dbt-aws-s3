import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit, when
import torch
from transformers import AutoTokenizer, AutoModel
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levellevelname)s - %(message)s', filename='consumer_app.log', filemode='w')
logger = logging.getLogger(__name__)

def create_spark_connection():
    try:
        spark = SparkSession.builder \
            .appName('GameResultsEmbeddings') \
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
    if table_name == "game_results":
        df = df.withColumn("descriptive_text", concat_ws(". ",
            lit("Game ID"), col("gamePk"),
            lit("Season"), col("season"),
            lit("Official Date"), col("officialDate"),
            lit("Away Team"), col("away_team_name"),
            lit("Home Team"), col("home_team_name"),
            lit("Away Team Score"), col("away_team_away_score"),
            lit("Home Team Score"), col("home_team_home_score"),
            lit("Away Team Winner"), when(col("away_team_away_isWinner") == True, "Yes").otherwise("No"),
            lit("Home Team Winner"), when(col("home_team_home_isWinner") == True, "Yes").otherwise("No"),
            lit("Key Players"), col("away_pitcher_fullName"), lit(" and "), col("home_pitcher_fullName")
        ))
    elif table_name == "dim_teams":
        df = df.withColumn("descriptive_text", concat_ws(". ",
            lit("Team ID"), col("team_id"),
            lit("Team Name"), col("team_name"),
            lit("Location"), col("location_name"),
            lit("League"), col("league_name"),
            lit("Division"), col("division_name"),
            lit("Sport"), col("sport_name"),
            lit("First Year of Play"), col("first_year_of_play"),
            lit("Active"), when(col("active") == True, "Yes").otherwise("No")
        ))
    elif table_name == "fact_game_results":
        df = df.withColumn("descriptive_text", concat_ws(". ",
            lit("Game ID"), col("game_id"),
            lit("Season"), col("year"),
            lit("Official Date"), col("official_date"),
            lit("Home Team ID"), col("home_team_id"),
            lit("Away Team ID"), col("away_team_id"),
            lit("Home Team Score"), col("home_score"),
            lit("Away Team Score"), col("away_score"),
            lit("Home Team Winner"), when(col("home_is_winner") == True, "Yes").otherwise("No"),
            lit("Away Team Winner"), when(col("away_is_winner") == True, "Yes").otherwise("No")
        ))
    elif table_name == "future_games":
        df = df.withColumn("descriptive_text", concat_ws(". ",
            lit("Game ID"), col("gamePk"),
            lit("Season"), col("season"),
            lit("Official Date"), col("officialDate"),
            lit("Home Team"), col("home_team_name"),
            lit("Home Team Winner"), when(col("home_team_home_isWinner") == True, "Yes").otherwise("No"),
            lit("Away Team"), col("away_team_name"),
            lit("Away Team Winner"), when(col("away_team_away_isWinner") == True, "Yes").otherwise("No"),
            lit("Home Team Score"), col("home_team_home_score"),
            lit("Away Team Score"), col("away_team_away_score")
        ))
    elif table_name in ["season_totals", "all_seasons_totals"]:
        df = df.withColumn("descriptive_text", concat_ws(". ",
            lit("Season"), col("year"),
            lit("Team"), col("team_id"),
            lit("Total Away Score"), col("total_away_score"),
            lit("Total Home Score"), col("total_home_score")
        ))
    elif table_name in ["season_averages", "all_seasons_averages"]:
        df = df.withColumn("descriptive_text", concat_ws(". ",
            lit("Season"), col("year"),
            lit("Team"), col("team_id"),
            lit("Average Away Score"), col("avg_away_score"),
            lit("Average Home Score"), col("avg_home_score")
        ))
    return df

def generate_and_store_embeddings(df, table_name, collection_name):
    model_name = "sentence-transformers/all-MiniLM-L6-v2"
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModel.from_pretrained(model_name)

    qdrant_client = QdrantClient(host="qdrant", port=6333)

    for row in df.collect():
        inputs = tokenizer(row.descriptive_text, return_tensors='pt', truncation=True, padding=True)
        with torch.no_grad():
            embeddings = model(**inputs).last_hidden_state.mean(dim=1).numpy()

        point = PointStruct(
            id=row.gamePk if table_name == "game_results" or table_name == "future_games" else row.team_id,
            vector=embeddings.tolist()[0],
            payload={"descriptive_text": row.descriptive_text, "table": table_name}
        )
        qdrant_client.upsert(collection_name=collection_name, points=[point])
        logger.info(f"Stored embedding for row ID: {row.gamePk if table_name == 'game_results' or table_name == 'future_games' else row.team_id} from table {table_name} in Qdrant.")

def process_and_store_embeddings(spark, table_name, collection_name):
    df = spark.read.format("iceberg").load(f"hdfs://namenode:8020/warehouse/mlb_db/{table_name}")
    df = transform_to_descriptive_text(df, table_name)
    generate_and_store_embeddings(df, table_name, collection_name)

def main():
    try:
        spark = create_spark_connection()

        # Define the collection name
        collection_name = "gameresults_info"

        tables = ["game_results", "dim_teams", "fact_game_results", "future_games", "current_season_averages",  
                  "current_season_totals", "season_totals", "all_seasons_totals", "season_averages", "all_seasons_averages"]

        for table_name in tables:
            process_and_store_embeddings(spark, table_name, collection_name)
        
        spark.stop()
        logger.info("Main function executed successfully.")
    except Exception as e:
        logger.error(f"Error in main function: {e}", exc_info=True)

if __name__ == "__main__":
    main()
