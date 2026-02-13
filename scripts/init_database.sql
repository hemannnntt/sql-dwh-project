/*
===========================
Create Datavase and Schemas
===========================
Script Purpose:
  This script creates a new database named 'DataWareHouse' after checking if it already exists.
  If the dabase exists, it is dropped and recreated. Additionaly, the script sets up three schemas
  within the database: 'bronze', 'silver', 'gold'.

WARNING:
  Running this script will drop the entire 'DataWareHouse' database if it exists.
  All data in the database will be permanently deleted. Proceed with caution and ensure
  you have proper backups before running this script.
*/


--Create Database 'DataWareHouse'
USE master;
GO

-- Drop and recreate the 'DataWareHouse' databases.
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWareHouse')
BEGIN
    ALTER DATABASE DataWareHouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWareHouse;
END;
GO

-- Create the 'DataWareHouse' database
CREATE DATABASE DataWareHouse;

USE DataWareHouse;

-- Create schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
