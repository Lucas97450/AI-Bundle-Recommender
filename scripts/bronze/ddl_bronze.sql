/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

-- Category_tree Table
IF OBJECT_ID ('bronze.category_tree', 'U') IS NOT NULL
        DROP TABLE bronze.category_tree;
GO

CREATE TABLE bronze.category_tree (
category_id   INT,
parent_id     INT
);

GO

-- Envents Table
IF OBJECT_ID ('bronze.events', 'U') IS NOT NULL
        DROP TABLE bronze.events;
GO

CREATE TABLE bronze.events (
timestamp      BIGINT,
visitor_id     BIGINT,
event          NVARCHAR(50),
item_id        BIGINT,
transaction_id BIGINT	

)

GO

-- Item Properties Table part 1
IF OBJECT_ID ('bronze.item_properties_part_1', 'U') IS NOT NULL
        DROP TABLE bronze.item_properties_part_1;
GO

CREATE TABLE bronze.item_properties_part_1(
timestamp     BIGINT,
itemid        BIGINT,
property      VARCHAR(100),
value         VARCHAR(MAX)
)

GO

-- Item Properties Table part 2
IF OBJECT_ID ('bronze.item_properties_part_2', 'U') IS NOT NULL
        DROP TABLE bronze.item_properties_part_2;
GO

CREATE TABLE bronze.item_properties_part_2(
timestamp     BIGINT,
itemid        BIGINT,
property      VARCHAR(100),
value         VARCHAR(MAX)
)

GO
