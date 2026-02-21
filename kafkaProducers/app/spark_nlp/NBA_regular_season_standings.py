# file: season_standings_spark_nlp.py
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
            SparkSession.builder.appName("Season_Standings_SparkNLP")
            .enableHiveSupport()
            .getOrCreate()
        )
        logger.info("Spark connection created for season standings Spark NLP.")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark connection: {e}", exc_info=True)
        sys.exit(1)


def load_season_standings(spark: SparkSession) -> DataFrame:
    """
    Loads from nba_db.nba_overall or whichever table you use for standings.
    """
    try:
        columns_of_interest: List[str] = [
            "TeamID",
            "TeamName",
            "WINS",
            "LOSSES",
            "WinPCT",
            "PointsPG"
        ]
        df: DataFrame = spark.table("nba_db.nba_overall").select(*columns_of_interest)
        logger.info("Loaded season standings from nba_overall.")
        return df
    except Exception as e:
        logger.error(f"Error loading season standings: {e}", exc_info=True)
        sys.exit(1)


def create_textual_summary(df: DataFrame) -> DataFrame:
    try:
        text_df = df.withColumn(
            "standings_summary",
            concat_ws(
                " ",
                lit("Team"),
                col("TeamName"),
                lit("has"),
                col("WINS").cast("string"),
                lit("wins and"),
                col("LOSSES").cast("string"),
                lit("losses, WinPCT of"),
                col("WinPCT").cast("string"),
                lit(", PointsPG:"),
                col("PointsPG").cast("string")
            )
        )
        return text_df
    except Exception as e:
        logger.error(f"Error creating textual summary: {e}", exc_info=True)
        sys.exit(1)


def build_spark_nlp_pipeline() -> Pipeline:
    document_assembler = DocumentAssembler() \
        .setInputCol("standings_summary") \
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
            "TeamID", "TeamName", "finished_embeddings"
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
            team_id = int(row["TeamID"])
            metadata = {"TeamName": row["TeamName"]}
            points.append(PointStruct(id=team_id, vector=vector, payload=metadata))

        qdrant_client.upsert(collection_name=collection_name, points=points)
        logger.info(f"Stored {len(points)} season standings embeddings into Qdrant.")
    except Exception as e:
        logger.error(f"Error storing embeddings in Qdrant: {e}", exc_info=True)
        sys.exit(1)


def main() -> None:
    spark = create_spark_connection()
    df_standings = load_season_standings(spark)
    text_df = create_textual_summary(df_standings)

    pipeline = build_spark_nlp_pipeline()
    model = pipeline.fit(text_df)
    result_df = model.transform(text_df)

    store_embeddings_in_qdrant(result_df, "season_standings_embeddings")

    spark.stop()
    logger.info("Season standings Spark NLP script completed successfully.")


if __name__ == "__main__":
    main()
