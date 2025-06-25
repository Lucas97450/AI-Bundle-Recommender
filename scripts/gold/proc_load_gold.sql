/*
===============================================================================
Stored Procedure: Load Gold Layer (Silver -> Gold)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'gold' schema from the silver layer. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from silver tables to gold tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC gold.load_gold;
===============================================================================
*/
CREATE OR ALTER PROCEDURE gold.load_gold AS
BEGIN

    DECLARE @batch_start DATETIME = GETDATE(),
            @step_start  DATETIME,
            @step_end    DATETIME;

    BEGIN TRY
        PRINT '================================================';
        PRINT 'Loading Gold Layer (fact_events, dim_item, dim_category)';
        PRINT '================================================';

        SET @step_start = GETDATE();
        PRINT '>> Upsert gold.fact_events (incrémental)';

        DECLARE @last DATETIME2(3) =
            ISNULL((SELECT MAX(event_ts) FROM gold.fact_events), '19000101');

        INSERT INTO gold.fact_events (event_ts, visitor_id, item_id,
                                      category_id, is_available, event_type)
        SELECT
            event_ts ,
            visitor_id ,
            item_id ,
            category_id ,
            is_available ,
            event_type
        FROM (
                SELECT
                    e.[timestamp]                          AS event_ts,
                    e.visitor_id,
                    e.item_id,
                    ISNULL(c.category_id , -1)            AS category_id,
                    ISNULL(a.is_available , 0)            AS is_available,
                    e.[event]                             AS event_type,
                    ROW_NUMBER() OVER (
                        PARTITION BY e.[timestamp], e.visitor_id, e.item_id
                        ORDER BY (SELECT 0)
                    ) AS rn
                FROM silver.events               AS e
                LEFT JOIN silver.item_category   AS c ON c.item_id = e.item_id
                LEFT JOIN silver.item_availability a ON a.item_id = e.item_id
                WHERE e.[timestamp] > @last
        ) AS x
        WHERE rn = 1;          


        SET @step_end = GETDATE();
        PRINT '   -> inserted rows : ' + CAST(@@ROWCOUNT AS NVARCHAR);
        PRINT '   -> duration      : ' + CAST(DATEDIFF(ms,@step_start,@step_end) AS NVARCHAR) + ' ms';


        SET @step_start = GETDATE();
        PRINT '>> Refresh gold.dim_item';

        TRUNCATE TABLE gold.dim_item;

        INSERT INTO gold.dim_item (item_id, category_id, is_available)
        SELECT
            a.item_id,
            ISNULL(c.category_id,-1) AS category_id,
            a.is_available
        FROM   silver.item_availability AS a
        LEFT   JOIN silver.item_category AS c ON c.item_id = a.item_id;

        SET @step_end = GETDATE();
        PRINT '   -> rowcount : ' + CAST(@@ROWCOUNT AS NVARCHAR);
        PRINT '   -> duration : ' + CAST(DATEDIFF(ms,@step_start,@step_end) AS NVARCHAR) + ' ms';


        SET @step_start = GETDATE();
        PRINT '>> Refresh gold.dim_category';

        TRUNCATE TABLE gold.dim_category;

        INSERT INTO gold.dim_category (category_id, parent_id, is_root)
        SELECT category_id, parent_id, is_root
        FROM   silver.category_tree;

        SET @step_end = GETDATE();
        PRINT '   -> rowcount : ' + CAST(@@ROWCOUNT AS NVARCHAR);
        PRINT '   -> duration : ' + CAST(DATEDIFF(ms,@step_start,@step_end) AS NVARCHAR) + ' ms';

        DECLARE @batch_end DATETIME = GETDATE();
        PRINT '================================================';
        PRINT 'Gold Layer load completed successfully';
        PRINT 'Total duration : ' 
              + CAST(DATEDIFF(SECOND,@batch_start,@batch_end) AS NVARCHAR) 
              + ' seconds';
        PRINT '================================================';

    END TRY

    BEGIN CATCH
        PRINT '================================================';
        PRINT '‼️ ERROR during Gold load';
        PRINT 'Message : ' + ERROR_MESSAGE();
        PRINT 'Number  : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'State   : ' + CAST(ERROR_STATE()  AS NVARCHAR);
        PRINT '================================================';
        THROW;
    END CATCH
END
GO
