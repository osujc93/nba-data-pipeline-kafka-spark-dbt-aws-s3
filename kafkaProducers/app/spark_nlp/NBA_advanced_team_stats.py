# file: advanced_teams_spark_nlp.py
# PEP 8 Compliant, includes explicit type hints.

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
    try:
        spark: SparkSession = (
            SparkSession.builder.appName("Advanced_Teams_SparkNLP")
            .enableHiveSupport()
            .getOrCreate()
        )
        logger.info("Spark connection created for advanced teams Spark NLP.")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark connection: {e}", exc_info=True)
        sys.exit(1)


def load_advanced_team_stats(spark: SparkSession) -> DataFrame:
    try:
        columns_of_interest: List[str] = [
            "TEAM_ID",
            "TEAM_NAME",
            "GP",
            "W",
            "L",
            "NET_RATING",
            "OFF_RATING",
            "DEF_RATING",
            "PACE",
            "Season",
            "Month"
        ]
        df: DataFrame = (
            spark.table("nba_db.advanced_team_stats")
            .filter(col("is_current") == True)
            .select(*columns_of_interest)
        )
        logger.info("Loaded advanced team stats from Iceberg.")
        return df
    except Exception as e:
        logger.error(f"Error loading advanced team stats: {e}", exc_info=True)
        sys.exit(1)


def create_textual_summary(df: DataFrame) -> DataFrame:
    try:
        # Example: turn numeric stats into a short textual summary
        text_df = df.withColumn(
            "team_summary",
            concat_ws(
                " ",
                lit("Team"),
                col("TEAM_NAME"),
                lit("played"),
                col("GP").cast("string"),
                lit("games, OffRating:"),
                col("OFF_RATING").cast("string"),
                lit("DefRating:"),
                col("DEF_RATING").cast("string"),
                lit("NetRating:"),
                col("NET_RATING").cast("string"),
                lit("PACE:"),
                col("PACE").cast("string")
            )
        )
        return text_df
    except Exception as e:
        logger.error(f"Error creating textual summary: {e}", exc_info=True)
        sys.exit(1)


def build_spark_nlp_pipeline() -> Pipeline:
    document_assembler = DocumentAssembler() \
        .setInputCol("team_summary") \
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
            "TEAM_ID", "TEAM_NAME", "finished_embeddings"
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
            team_id = int(row["TEAM_ID"])
            metadata = {"TEAM_NAME": row["TEAM_NAME"]}
            points.append(
                PointStruct(
                    id=team_id,
                    vector=vector,
                    payload=metadata
                )
            )

        qdrant_client.upsert(collection_name=collection_name, points=points)
        logger.info(f"Stored {len(points)} advanced team embeddings into Qdrant.")
    except Exception as e:
        logger.error(f"Error storing embeddings in Qdrant: {e}", exc_info=True)
        sys.exit(1)


def main() -> None:
    spark = create_spark_connection()
    df_teams = load_advanced_team_stats(spark)
    text_df = create_textual_summary(df_teams)

    pipeline = build_spark_nlp_pipeline()
    model = pipeline.fit(text_df)
    result_df = model.transform(text_df)

    store_embeddings_in_qdrant(result_df, "advanced_teams_embeddings")

    spark.stop()
    logger.info("Advanced teams Spark NLP script completed successfully.")


if __name__ == "__main__":
    main()
