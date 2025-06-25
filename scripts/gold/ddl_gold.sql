/*
===============================================================================
DDL Script: Create Gold Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'gold' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'gold' Tables
===============================================================================
*/

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold');
GO


IF OBJECT_ID('gold.fact_events','U') IS NOT NULL
    DROP TABLE gold.fact_events;

GO

CREATE TABLE gold.fact_events (
        event_ts     DATETIME2(3) NOT NULL,
        visitor_id   BIGINT       NOT NULL,
        item_id      BIGINT       NOT NULL,
        category_id  INT          NOT NULL,   -- -1 = Unknown
        is_available BIT          NOT NULL,   -- 1 / 0
        event_type   VARCHAR(20)  NOT NULL,   -- view / addtocart / transaction
        CONSTRAINT PK_fact_events PRIMARY KEY CLUSTERED (event_ts, item_id, visitor_id)
    );
    CREATE INDEX ix_fact_events_item  ON gold.fact_events (item_id);

GO


IF OBJECT_ID('gold.fact_baskets','U') IS NOT NULL
    DROP TABLE gold.fact_baskets;
GO

    CREATE TABLE gold.fact_baskets (
        transaction_id BIGINT       NOT NULL PRIMARY KEY,
        basket_ts      DATETIME2(3) NOT NULL,
        items          NVARCHAR(MAX) NOT NULL
    );
       

GO


IF OBJECT_ID('gold.dim_item','U') IS NOT NULL
    DROP TABLE gold.dim_item;
GO

CREATE TABLE gold.dim_item (
    item_id       BIGINT       NOT NULL PRIMARY KEY,
    category_id   INT          NOT NULL,
    is_available  BIT          NOT NULL,
    );

GO


IF OBJECT_ID('gold.dim_category','U') IS NOT NULL
        DROP TABLE gold.dim_category;

GO

CREATE TABLE gold.dim_category (
        category_id INT  NOT NULL PRIMARY KEY,
        parent_id   INT  NULL,
        is_root     BIT  NOT NULL
);

GO


IF OBJECT_ID('gold.dim_calendar','U') IS NOT NULL
    DROP TABLE gold.dim_calendar;
GO

CREATE TABLE gold.dim_calendar (
        date_key DATE PRIMARY KEY,
        year      SMALLINT,
        quarter   TINYINT,
        month     TINYINT,
        day       TINYINT,
        dow       TINYINT,
        week_iso  SMALLINT
);

GO


