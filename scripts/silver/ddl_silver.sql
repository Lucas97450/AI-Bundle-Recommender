/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

CREATE SCHEMA silver;
GO

-- Category_tree Table
IF OBJECT_ID ('silver.category_tree', 'U') IS NOT NULL
        DROP TABLE silver.category_tree;
GO

CREATE TABLE silver.category_tree (
category_id   INT,
parent_id     INT,
is_root	      INT
);

GO

-- Envents Table
IF OBJECT_ID ('silver.events', 'U') IS NOT NULL
        DROP TABLE silver.events;
GO

CREATE TABLE silver.events (
timestamp      DATETIME,
visitor_id     INT,
event          NVARCHAR(50),
item_id        INT,
transaction_id INT

)

GO

-- Item Properties kv Table
IF OBJECT_ID ('silver.item_properties_kv', 'U') IS NOT NULL
        DROP TABLE silver.item_properties_kv;
GO

CREATE TABLE silver.item_properties_kv (
        snapshot_ts DATETIME      NULL,
        item_id     BIGINT        NULL,
        prop_key    NVARCHAR(100) NOT NULL,
        prop_val    NVARCHAR(MAX) NULL
)

GO

-- Item category Table
IF OBJECT_ID('silver.item_category','U') IS NOT NULL
	DROP TABLE silver.item_category;
GO
	
CREATE TABLE silver.item_category (
        item_id     BIGINT PRIMARY KEY,
        category_id INT
)

GO

-- Item availability Table
IF OBJECT_ID('silver.item_availability','U') IS NOT NULL
	DROP TABLE silver.item_availability
GO
	
CREATE TABLE silver.item_availability (
        item_id      BIGINT PRIMARY KEY,
        is_available BIT
)

GO

