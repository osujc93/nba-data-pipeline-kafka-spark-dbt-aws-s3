{{ config(
    materialized='table',
) }}

WITH base AS (
    SELECT