# file: traditional_players_spark_nlp.py
# PEP 8 Compliant, with explicit type hints.

import logging
import sys
from typing import List, Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import concat_ws, col, lit
from sparknlp.base import DocumentAssembler, EmbeddingsFinisher
from sparknlp.annotator import Tokenizer, BertEmbeddings
from pyspark.ml import Pipeline

from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger: logging.Logger = logging.getLogger(__name__)


def create_spark_connection() -> SparkSession:
    """
    Creates SparkSession with Hive support for reading from Iceberg.
    """
    try:
        spark: SparkSession = (
            SparkSession.builder.appName("Traditional_Players_SparkNLP")
            .enableHiveSupport()
            .getOrCreate()
        )
        logger.info("Spark connection created for traditional players Spark NLP.")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark connection: {e}", exc_info=True)
        sys.exit(1)


def load_traditional_player_stats(spark: SparkSession) -> DataFrame:
    """
    Loads from nba_db.player_stats, focusing on columns relevant for textual generation.
    """
    try:
        columns_of_interest: List[str] = [
            "PLAYER_ID",
            "PLAYER_NAME",
            "GP",
            "W",
            "L",
            "PTS",
            "REB",
            "FG_PCT"
        ]
        df: DataFrame = (
            spark.table("nba_db.player_stats")
            .filter(col("is_current") == True)
            .select(*columns_of_interest)
        )
        logger.info("Loaded traditional player stats from Iceberg.")
        return df
    except Exception as e:
        logger.error(f"Error loading player stats: {e}", exc_info=True)
        sys.exit(1)


def create_textual_summary(df: DataFrame) -> DataFrame:
    """
    Example textual summary for each player using some numeric columns.
    """
    try:
        text_df: DataFrame = df.withColumn(
            "player_summary",
            concat_ws(
                " ",
                lit("Player"),
                col("PLAYER_NAME"),
                lit("played"),
                col("GP").cast("string"),
                lit("games, averaging"),
                col("PTS").cast("string"),
                lit("points per game, FG%:"),
                col("FG_PCT").cast("string"),
                lit(", REB:"),
                col("REB").cast("string")
            )
        )
        return text_df
    except Exception as e:
        logger.error(f"Error creating textual summary: {e}", exc_info=True)
        sys.exit(1)


def build_spark_nlp_pipeline() -> Pipeline:
    document_assembler = DocumentAssembler() \
        .setInputCol("player_summary") \
        .setOutputCol("document")

    tokenizer = Tokenizer() \
        .setInputCols(["document"]) \
        .setOutputCol("token")

    bert = BertEmbeddings \
        .pretrained("small_bert_L2_128") \
        .setInputCols(["document", "token"]) \
        .setOutputCol("embeddings") \
        .setCaseSensitive(False)

    finisher = EmbeddingsFinisher() \
        .setInputCols(["embeddings"]) \
        .setOutputCols(["finished_embeddings"]) \
        .setOutputAsVector(True)

    pipeline = Pipeline(stages=[document_assembler, tokenizer, bert, finisher])
    return pipeline


def store_embeddings_in_qdrant(df: DataFrame, collection_name: str) -> None:
    try:
        local_data: List[Dict[str, Any]] = df.select(
            "PLAYER_ID", "PLAYER_NAME", "finished_embeddings"
        ).collect()

        qdrant_client = QdrantClient(host="qdrant", port=6333)
        qdrant_client.recreate_collection(
            collection_name=collection_name,
            vector_size=len(local_data[0]["finished_embeddings"][0]),
            distance="Cosine"
        )

        points = []
        for row in local_data:
            vector = row["finished_embeddings"][0]
            player_id = int(row["PLAYER_ID"])
            metadata = {"PLAYER_NAME": row["PLAYER_NAME"]}
            points.append(
                PointStruct(
                    id=player_id,
                    vector=vector,
                    payload=metadata
                )
            )

        qdrant_client.upsert(collection_name=collection_name, points=points)
        logger.info(f"Stored {len(points)} traditional player embeddings into Qdrant.")
    except Exception as e:
        logger.error(f"Error storing embeddings in Qdrant: {e}", exc_info=True)
        sys.exit(1)


def main() -> None:
    spark = create_spark_connection()
    df_players = load_traditional_player_stats(spark)
    text_df = create_textual_summary(df_players)

    pipeline = build_spark_nlp_pipeline()
    model = pipeline.fit(text_df)
    result_df = model.transform(text_df)

    store_embeddings_in_qdrant(result_df, "traditional_players_embeddings")

    spark.stop()
    logger.info("Traditional players Spark NLP script completed successfully.")


if __name__ == "__main__":
    main()
