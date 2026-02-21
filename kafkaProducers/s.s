# ------------------------- #
# 1. Import Dependencies
# ------------------------- #
import pmdarima as pm
import pandas as pd
from pyspark.sql.functions import collect_list, struct, asc, col, sort_array
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# For multiple-testing corrections:
from statsmodels.stats.multitest import multipletests

# >>> Add these two lines <<<
import warnings
warnings.filterwarnings("ignore", message=".*force_all_finite.*", category=FutureWarning)

# ------------------------- #
# 2. Spark Session
# ------------------------- #
spark = SparkSession.builder.getOrCreate()

# >>> Optionally reduce Spark logs <<<
#spark.sparkContext.setLogLevel("ERROR")

# ------------------------- #
# 3. Read Data
# ------------------------- #
df = spark.read.format("iceberg").load("spark_catalog.iceberg_nba_player_boxscores.scd_team_boxscores")

# ------------------------- #
# 4. (Optional) Filtering
#    Uncomment if you only
#    want a single player
# ------------------------- #
# Specify the single player_id you want to forecast
player_id_val = "1610612747"

# Filter the DataFrame so we only keep that single player
df_filtered = df.filter(col("team_id") == player_id_val)

df_filtered = df_filtered.dropDuplicates(["team_id", "game_id"])

# For now, let's just rename:
#df_filtered = df  # If you want all players, no filtering.

# ------------------------- #
# 5. Group & Collect Data
# ------------------------- #
grouped_df = (
    df_filtered
    .groupBy("team_id")
    .agg(
        sort_array(
            collect_list(
                struct(
                    col("game_date"),  # Keep original "game_date" column
                    col("team_points")
                )
            ),
            asc=True
        ).alias("collected_data")
    )
)

# Output structure:
# player_id | collected_data (array of structs {game_date, points})

# ------------------------- #
# 6. Fit ARIMA per Player
#    and gather stats
# ------------------------- #
full_results = []  # Will store final data for Spark
for row in grouped_df.collect():
    player_id_str = row["team_id"]
    collected_data = row["collected_data"]
    
    # Skip if empty
    if not collected_data:
        continue
    
    # Convert each Spark Row to dict -> Pandas DataFrame
    pdf = pd.DataFrame([r.asDict() for r in collected_data])
    # Sort by date for time-series
    pdf = pdf.sort_values(by="game_date")

    # Get the points as our target
    y = pdf["team_points"].fillna(0)

    # Need at least a few data points to train ARIMA
    if len(y) < 3:
        continue

    # ------------------------- #
    # Fit pmdarima model
    # ------------------------- #
    model = pm.auto_arima(
        y,
        start_p=1, start_q=1,
        max_p=5, max_q=5,
        seasonal=False,
        stepwise=True,
        trace=False
    )

    # Retrieve statsmodels ARIMA results from pmdarima:
    arima_res = model.arima_res_  # Typically works in modern pmdarima

    # ------------- Print Detailed Summary -------------
    print(f"\n=== Detailed Statsmodels Summary for player_id = {player_id_str} ===")
    print(arima_res.summary())

    # ------------- Multiple-Testing Corrections -------------
    # Extract p-values from the model
    p_values = arima_res.pvalues
    # Extract parameter names
    param_names = arima_res.params.index  # e.g., ['intercept', 'ar.L1', 'sigma2', etc.]

    # Apply Bonferroni
    bonferroni_results = multipletests(p_values, alpha=0.15, method='bonferroni')
    # Apply Benjamini-Hochberg
    bh_results = multipletests(p_values, alpha=0.15, method='fdr_bh')

    print("\nBonferroni Corrected p-values:")
    for name, orig_p, corrected_p in zip(param_names, p_values, bonferroni_results[1]):
        print(f"{name}: original p-value = {orig_p:.6f}, corrected p-value = {corrected_p:.6f}")

    print("\nBenjamini-Hochberg Corrected p-values:")
    for name, orig_p, corrected_p in zip(param_names, p_values, bh_results[1]):
        print(f"{name}: original p-value = {orig_p:.6f}, corrected p-value = {corrected_p:.6f}")

    # Create a small Pandas DataFrame summarizing ARIMA coefficients & p-value corrections
    df_results = pd.DataFrame({
        'ARIMA_Coefficient': param_names,
        'Coefficient_Value': arima_res.params.values,
        'Original_P_Value': p_values,
        'Bonferroni_Corrected_P': bonferroni_results[1],
        'BH_Corrected_P': bh_results[1]
    })

    print("\n=== ARIMA Coefficients & Corrected P-Values for player_id =", player_id_str, "===")
    print(df_results)

    # ------------- Collect minimal info for Spark DataFrame -------------
    arima_order_str = str(model.order)
    aic_val = float(model.aic())

    intercept_p = float(p_values["intercept"]) if "intercept" in p_values else None

    full_results.append((
        str(player_id_str),
        arima_order_str,
        aic_val,
        intercept_p
    ))

# ------------------------- #
# 7. Convert Results to Spark
# ------------------------- #
schema = StructType([
    StructField("team_id", StringType(), True),
    StructField("arima_order", StringType(), True),
    StructField("aic", FloatType(), True),
    StructField("intercept_p_value", FloatType(), True)
])

enhanced_results_df = spark.createDataFrame(full_results, schema=schema)

print("\n=== Final Spark DataFrame (ARIMA Results) ===")
enhanced_results_df.show(truncate=False)
