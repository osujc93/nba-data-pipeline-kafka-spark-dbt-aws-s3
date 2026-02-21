# file: advanced_players_spark_nlp.py
# PEP 8 Compliant, includes explicit type hints.
import sparknlp
from sparknlp.base import *
from sparknlp.annotator import *
from pyspark.ml import Pipeline

import logging
import sys
from typing import List, Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    concat_ws,
    when,
    monotonically_increasing_id
)

# Qdrant
# Make sure 'qdrant-client' is installed (pip install qdrant-client)
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger: logging.Logger = logging.getLogger(__name__)


def create_spark_connection() -> SparkSession:
    """
    Creates and returns a SparkSession with the necessary configurations for Spark NLP.
    """
    try:
        spark: SparkSession = (
            SparkSession.builder
            .appName("Advanced_Players_SparkNLP")
            .master("local[*]")
            .config("spark.driver.memory", "16G")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.kryoserializer.buffer.max", "2000M")
            .config("spark.driver.maxResultSize", "0")
            .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:5.5.1")         
            # You may need to set Spark NLP packages or jars, e.g.:
            # .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:5.5.2")
            .enableHiveSupport()
            .getOrCreate()
        )
        logger.info("Spark connection created successfully for advanced players Spark NLP.")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark connection: {e}", exc_info=True)
        sys.exit(1)


def load_advanced_player_stats(spark: SparkSession) -> DataFrame:
    """
    Loads advanced player stats from your Iceberg table: nba_db.advanced_player_stats.
    Returns the DataFrame with numeric columns for summary creation.
    """
    try:
        columns_of_interest: List[str] = [
            "PLAYER_ID",
            "PLAYER_NAME",
            "GP",
            "W",
            "L",
            "OFF_RATING",
            "DEF_RATING",
            "NET_RATING",
            "PIE",
            "PACE",
            "Season",
            "Month"
        ]
        df: DataFrame = (
            spark.table("nba_db.advanced_player_stats")
            .filter(col("is_current") == True)
            .select(*columns_of_interest)
        )
        logger.info("Loaded advanced player stats from Iceberg.")
        return df
    except Exception as e:
        logger.error(f"Error loading advanced player stats: {e}", exc_info=True)
        sys.exit(1)


def create_textual_summary(df: DataFrame) -> DataFrame:
    """
    Converts numeric stats into a short textual 'profile' string for each player.
    This text is what we'll embed using Spark NLP.
    """
    try:
        # Example simplistic approach: combine numeric stats into a descriptive string
        # Use concat_ws or your own custom logic to form a short summary
        text_df: DataFrame = df.withColumn(
            "player_summary",
            concat_ws(
                " ",
                lit("Player:"),
                col("PLAYER_NAME"),
                lit("has played"),
                col("GP").cast("string"),
                lit("games with OFF_RATING of"),
                col("OFF_RATING").cast("string"),
                lit("and DEF_RATING of"),
                col("DEF_RATING").cast("string"),
                lit(". PACE:"),
                col("PACE").cast("string"),
                lit("NET_RATING:"),
                col("NET_RATING").cast("string"),
                lit("PIE:"),
                col("PIE").cast("string")
            )
        )
        return text_df
    except Exception as e:
        logger.error(f"Error creating textual summary: {e}", exc_info=True)
        sys.exit(1)


def build_spark_nlp_pipeline() -> Pipeline:
    """
    Builds a simple Spark NLP pipeline:
    1) DocumentAssembler -> 2) Tokenizer -> 3) BertEmbeddings -> 4) EmbeddingsFinisher
    """
    document_assembler = DocumentAssembler() \
        .setInputCol("player_summary") \
        .setOutputCol("document")

    tokenizer = Tokenizer() \
        .setInputCols(["document"]) \
        .setOutputCol("token")

    # Example: BERT base embeddings
    # You must ensure spark-nlp and its models are available in your environment
    bert = BertEmbeddings \
        .pretrained("small_bert_L2_128") \
        .setInputCols(["document", "token"]) \
        .setOutputCol("embeddings") \
        .setCaseSensitive(False)

    # Convert spark-nlp embeddings into array
    finisher = EmbeddingsFinisher() \
        .setInputCols(["embeddings"]) \
        .setOutputCols(["finished_embeddings"]) \
        .setOutputAsVector(True)

    pipeline = Pipeline(stages=[document_assembler, tokenizer, bert, finisher])
    return pipeline


def store_embeddings_in_qdrant(df: DataFrame, collection_name: str) -> None:
    """
    Stores the resulting embeddings into Qdrant. Each row is a point with 'finished_embeddings'.
    Adjust Qdrant host/port as needed.
    """
    try:
        # Convert to local collection (since we need to gather embeddings)
        # This step triggers Spark action, collecting embeddings in the driver
        local_data: List[Dict[str, Any]] = df.select(
            "PLAYER_ID", "PLAYER_NAME", "finished_embeddings"
        ).collect()

        # Initialize Qdrant client
        qdrant_client = QdrantClient(host="qdrant", port=6333)  # or "localhost"

        # Create the collection if not exists
        qdrant_client.recreate_collection(
            collection_name=collection_name,
            vector_size=len(local_data[0]["finished_embeddings"][0]),  # size of each embedding
            distance="Cosine"
        )

        # Prepare points
        points = []
        for row in local_data:
            vector = row["finished_embeddings"][0]  # 1st sentence embedding (doc-level)
            player_id = int(row["PLAYER_ID"])
            metadata = {
                "PLAYER_NAME": row["PLAYER_NAME"]
            }
            points.append(
                PointStruct(
                    id=player_id,  # or use a unique ID
                    vector=vector,
                    payload=metadata
                )
            )

        # Upsert into Qdrant
        qdrant_client.upsert(
            collection_name=collection_name,
            points=points
        )
        logger.info(f"Successfully stored {len(points)} advanced player embeddings into Qdrant.")
    except Exception as e:
        logger.error(f"Error storing embeddings in Qdrant: {e}", exc_info=True)
        sys.exit(1)


def main() -> None:
    spark = create_spark_connection()
    df_advanced = load_advanced_player_stats(spark)
    text_df = create_textual_summary(df_advanced)

    # Build and fit Spark NLP pipeline
    pipeline = build_spark_nlp_pipeline()
    model = pipeline.fit(text_df)
    result_df = model.transform(text_df)

    # result_df now has "finished_embeddings" column
    # We'll pick doc-level embeddings from that array
    # For convenience, use only the doc-level (index 0) embedding
    store_embeddings_in_qdrant(result_df, collection_name="advanced_players_embeddings")

    spark.stop()
    logger.info("Advanced players Spark NLP script completed successfully.")


if __name__ == "__main__":
    main()
