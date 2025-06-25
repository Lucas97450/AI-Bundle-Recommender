/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';
		
		SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.category_tree';
			TRUNCATE TABLE silver.category_tree;
			PRINT '>> Inserting Data Into: silver.category_tree';
			INSERT INTO silver.category_tree (
				category_id,
				parent_id,
				is_root
			)
			SELECT
				category_id,
				CAST(NULLIF(TRIM(CAST(raw_parent_id AS VARCHAR)), '') AS INT) AS parent_id,
				CASE 
					WHEN TRIM(raw_parent_id) IS NULL OR TRIM(CAST(raw_parent_id AS VARCHAR)) = '' THEN 1
					ELSE 0
				END AS is_root
			FROM (
				SELECT
					CAST(TRIM(CAST(category_id AS VARCHAR)) AS INT) AS category_id,
					TRIM(CAST(parent_id AS VARCHAR)) AS raw_parent_id,
					ROW_NUMBER() OVER (
						PARTITION BY CAST(TRIM(CAST(category_id AS VARCHAR)) AS INT)
						ORDER BY parent_id
					) AS rn
				FROM bronze.category_tree
			) AS cleaned
			WHERE rn = 1;
		SET @end_time = GETDATE();

		SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.events';
			TRUNCATE TABLE silver.events
			PRINT '>> Inserting Data Into: silver.events';
		INSERT INTO silver.events(
				timestamp,
				visitor_id,
				event,
				item_id,
				transaction_id
		)
		SELECT
		  DATEADD(SECOND, CAST(timestamp / 1000 AS BIGINT), '1970-01-01') AS timestamp,
		  CAST(visitor_id AS VARCHAR(64)) AS visitor_id,
		  LOWER(event) AS event_type,
		  CAST(item_id AS INT) AS item_id,
		  CASE 
			WHEN event = 'transaction' THEN CAST(transaction_id AS INT)
			ELSE NULL
		  END AS transaction_id
		FROM bronze.events
		WHERE visitor_id IS NOT NULL AND item_id IS NOT NULL;
		SET @end_time = GETDATE();

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.item_properties_kv_raw';
		DROP TABLE IF EXISTS silver.item_properties_kv_raw;

		CREATE TABLE silver.item_properties_kv_raw
		(
			snapshot_ts BIGINT          NULL,
			item_id     BIGINT          NULL,
			prop_key    NVARCHAR(100)   NULL,
			prop_val    NVARCHAR(MAX)   NULL
		);

		PRINT '>> Inserting Data Into: silver.item_properties_kv_raw';
		INSERT INTO silver.item_properties_kv_raw (snapshot_ts, item_id, prop_key, prop_val)
		SELECT timestamp   ,
			   itemid      ,
			   property    ,
			   value
		FROM   bronze.item_properties_part_1
		UNION ALL
		SELECT timestamp, 
			   itemid,
			   property,
			   value
		FROM   bronze.item_properties_part_2;

		PRINT '>> Truncating Table: silver.item_properties_kv';
		TRUNCATE TABLE silver.item_properties_kv
		PRINT '>> Inserting Data Into: silver.item_properties_kv';
		;WITH ranked AS
		(
		  SELECT
			  snapshot_ts,
			  TRY_CAST(item_id  AS BIGINT)                   AS item_id,
			  LOWER(LTRIM(RTRIM(prop_key)))                  AS prop_key,
			  LTRIM(RTRIM(prop_val))                         AS prop_val,
			  ROW_NUMBER() OVER (PARTITION BY
							TRY_CAST(item_id AS BIGINT),
							LOWER(LTRIM(RTRIM(prop_key))),
							LTRIM(RTRIM(prop_val))
						   ORDER BY snapshot_ts)        AS rn
			 FROM silver.item_properties_kv_raw
		)
		INSERT INTO silver.item_properties_kv (snapshot_ts, item_id, prop_key, prop_val)
		SELECT DATEADD(SECOND,  CAST(snapshot_ts / 1000 AS INT), '1970-01-01')  AS snapshot_ts,
			   item_id,
			   prop_key,
			   prop_val
		FROM   ranked
		WHERE  rn = 1;
		SET @end_time = GETDATE();

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.item_category';
		TRUNCATE TABLE silver.item_category;

		PRINT '>> Inserting Data Into: silver.item_category';
		;WITH src AS (
			SELECT item_id,
				   TRY_CAST(prop_val AS INT) AS category_id,
				   snapshot_ts,
				   ROW_NUMBER() OVER (
					   PARTITION BY item_id
					   ORDER BY snapshot_ts DESC
				   ) AS rn
			FROM silver.item_properties_kv
			WHERE prop_key = 'categoryid'
			  AND TRY_CAST(prop_val AS INT) IS NOT NULL
		)
		INSERT INTO silver.item_category (item_id, category_id)
		SELECT item_id, category_id
		FROM   src
		WHERE  rn = 1;

		PRINT '>> Truncating Table: silver.item_availability';
		TRUNCATE TABLE silver.item_availability;
		PRINT '>> Inserting Data Into: silver.item_availability';
		;WITH src AS (
			SELECT
				item_id,
				CASE WHEN prop_val = '1' THEN 1 ELSE 0 END        AS is_available,
				snapshot_ts,
				ROW_NUMBER() OVER (
					PARTITION BY item_id
					ORDER BY snapshot_ts DESC
				) AS rn
			FROM silver.item_properties_kv
			WHERE prop_key = 'available'
		)
		INSERT INTO silver.item_availability (item_id, is_available)
		SELECT item_id, is_available
		FROM   src
		WHERE  rn = 1;
		SET @end_time = GETDATE();

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
		
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
