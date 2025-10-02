{{ config(
    materialized='table'
) }}

SELECT * FROM transactional.accounts