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

		SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.events';
			TRUNCATE TABLE silver.category_tree;
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
