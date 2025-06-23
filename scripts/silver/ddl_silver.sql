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
parent_id     INT
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

-- Item Properties Table part 1
IF OBJECT_ID ('silver.item_properties_part_1', 'U') IS NOT NULL
        DROP TABLE silver.item_properties_part_1;
GO

CREATE TABLE silver.item_properties_part_1(
timestamp     BIGINT,
itemid        BIGINT,
property      VARCHAR(100),
value         VARCHAR(225)
)

GO

-- Item Properties Table part 2
IF OBJECT_ID ('silver.item_properties_part_2', 'U') IS NOT NULL
        DROP TABLE silver.item_properties_part_2;
GO

CREATE TABLE silver.item_properties_part_2(
timestamp     BIGINT,
itemid        BIGINT,
property      VARCHAR(100),
value         VARCHAR(225)
)

GO
