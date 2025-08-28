BEGIN TRAN create_etl_user;

CREATE LOGIN etl_login
WITH PASSWORD = 'IowaETL456&';
GO

CREATE USER etl_user
FOR LOGIN etl_login;
GO

USE [Iowa_Sales_Data_Warehouse]
GRANT SELECT ON OBJECT::dbo.DimCounty
	TO etl_user;
GRANT INSERT ON OBJECT::dbo.DimCounty
	TO etl_user;

GRANT SELECT ON OBJECT::DimDateTable
	TO etl_user;
GRANT INSERT ON OBJECT::DimDateTable
	TO etl_user;

GRANT SELECT ON OBJECT::dbo.DimPackaging
	TO etl_user;
GRANT INSERT ON OBJECT::dbo.DimPackaging
	TO etl_user;

GRANT SELECT ON OBJECT::dbo.DimVendor
	TO etl_user;
GRANT INSERT ON OBJECT::dbo.DimVendor
	TO etl_user;

GRANT SELECT ON OBJECT::dbo.DimItem
	TO etl_user;
GRANT INSERT ON OBJECT::dbo.DimItem
	TO etl_user;

GRANT SELECT ON OBJECT::dbo.DimStore
	TO etl_user;
GRANT INSERT ON OBJECT::dbo.DimStore
	TO etl_user;

GRANT SELECT ON OBJECT::dbo.FLiquorSales
	TO etl_user;
GRANT INSERT ON OBJECT::dbo.FLiquorSales
	TO etl_user;


COMMIT TRAN create_etl_user;