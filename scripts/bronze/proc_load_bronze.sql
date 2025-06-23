/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @global_start_time DATETIME, @step_start_time DATETIME, @end_time DATETIME;
	SET @global_start_time = GETDATE();
    BEGIN TRY
			PRINT '===========================================';
			PRINT 'Loading Bronze Layer';
			PRINT '===========================================';

			SET @step_start_time = GETDATE();
			PRINT '>> Truncating Table: bronze.category_tree'
			TRUNCATE TABLE bronze.category_tree;

			PRINT '>>  Inserting data into: bronze.category_tree'
            BULK INSERT bronze.category_tree
            FROM 'dataset-path'
            WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				ROWTERMINATOR = '0x0a',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT '--------------------';
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @step_start_time, @end_time) AS NVARCHAR) + ' millisecond';
			PRINT '--------------------';

			SET @step_start_time = GETDATE();
			PRINT '>> Truncating Table: bronze.events'
			TRUNCATE TABLE bronze.events;

			DROP TABLE IF EXISTS events_raw;

			CREATE TABLE events_raw (
				col1 NVARCHAR(MAX),
				col2 NVARCHAR(MAX),
				col3 NVARCHAR(MAX),
				col4 NVARCHAR(MAX)
			);

			BULK INSERT events_raw
			FROM 'dataset-path'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				ROWTERMINATOR = '0x0a',
				TABLOCK
			);

			PRINT '>>  Inserting data into: bronze.events'
            INSERT INTO bronze.events (timestamp, visitor_id, event, item_id)
			SELECT
				TRY_CAST(col1 AS BIGINT),
				TRY_CAST(col2 AS BIGINT),
				col3,
				TRY_CAST(col4 AS BIGINT)
			FROM events_raw;

			SET @end_time = GETDATE();

			PRINT '--------------------';
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @step_start_time, @end_time) AS NVARCHAR) + ' millisecond';
			PRINT '--------------------';

			SET @step_start_time = GETDATE();
			PRINT '>> Truncating Table: bronze.item_properties_part_1'
			TRUNCATE TABLE bronze.item_properties_part_1;

			PRINT '>>  Inserting data into: bronze.item_properties_part_1'
            BULK INSERT bronze.item_properties_part_1
            FROM 'dataset-path'
            WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				ROWTERMINATOR = '0x0a',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT '--------------------';
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @step_start_time, @end_time) AS NVARCHAR) + ' millisecond';
			PRINT '--------------------';

			SET @step_start_time = GETDATE();
			PRINT '>> Truncating Table: bronze.item_properties_part_2'
			TRUNCATE TABLE bronze.item_properties_part_2;

			PRINT '>>  Inserting data into: bronze.item_properties_part_2'
            BULK INSERT bronze.item_properties_part_2
            FROM 'dataset-path'
            WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				ROWTERMINATOR = '0x0a',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT '--------------------';
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @step_start_time, @end_time) AS NVARCHAR) + ' millisecond';
			PRINT '--------------------';


    END TRY
	BEGIN CATCH
		PRINT '===========================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Number' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '===========================================';
	END CATCH
	SET @end_time = GETDATE();
	PRINT '--------------------';
	PRINT '>> Bronze Layer Load Duration: ' + CAST(DATEDIFF(millisecond, @global_start_time, @end_time) AS NVARCHAR) + ' milliseconds';
	PRINT '--------------------';
END
