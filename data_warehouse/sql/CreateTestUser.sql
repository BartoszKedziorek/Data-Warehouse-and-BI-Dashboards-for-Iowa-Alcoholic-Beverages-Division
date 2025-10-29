BEGIN TRAN create_test_user;

CREATE LOGIN test_login
WITH PASSWORD = 'IowaETL456';
GO

CREATE USER test_user
FOR LOGIN test_login;
GO

USE [Iowa_Sales_Data_Warehouse_test]
GRANT SELECT ON OBJECT::dbo.DimCounty
	TO test_user;
GRANT INSERT ON OBJECT::dbo.DimCounty
	TO test_user;
GRANT ALTER ON OBJECT::dbo.DimCounty
	TO test_user;
GRANT DELETE ON OBJECT::dbo.DimCounty
	TO test_user;
GRANT UPDATE ON OBJECT::dbo.DimCounty
	TO test_user;	

GRANT SELECT ON OBJECT::DimDateTable
	TO test_user;
GRANT INSERT ON OBJECT::DimDateTable
	TO test_user;
GRANT ALTER ON OBJECT::DimDateTable
	TO test_user;
GRANT DELETE ON OBJECT::DimDateTable
	TO test_user;
GRANT UPDATE ON OBJECT::DimDateTable
	TO test_user;

GRANT SELECT ON OBJECT::dbo.DimPackaging
	TO test_user;
GRANT INSERT ON OBJECT::dbo.DimPackaging
	TO test_user;
GRANT ALTER ON OBJECT::dbo.DimPackaging
	TO test_user;
GRANT DELETE ON OBJECT::dbo.DimPackaging
	TO test_user;
GRANT UPDATE ON OBJECT::dbo.DimPackaging
	TO test_user;

GRANT SELECT ON OBJECT::dbo.DimVendor
	TO test_user;
GRANT INSERT ON OBJECT::dbo.DimVendor
	TO test_user;
GRANT ALTER ON OBJECT::dbo.DimVendor
	TO test_user;
GRANT DELETE ON OBJECT::dbo.DimVendor
	TO test_user;
GRANT UPDATE ON OBJECT::dbo.DimVendor
	TO test_user;

GRANT SELECT ON OBJECT::dbo.DimItem
	TO test_user;
GRANT INSERT ON OBJECT::dbo.DimItem
	TO test_user;
GRANT ALTER ON OBJECT::dbo.DimItem
	TO test_user;
GRANT DELETE ON OBJECT::dbo.DimItem
	TO test_user;
GRANT UPDATE ON OBJECT::dbo.DimItem
	TO test_user;

GRANT SELECT ON OBJECT::dbo.DimStore
	TO test_user;
GRANT INSERT ON OBJECT::dbo.DimStore
	TO test_user;
GRANT ALTER ON OBJECT::dbo.DimStore
	TO test_user;
GRANT DELETE ON OBJECT::dbo.DimStore
	TO test_user;
GRANT UPDATE ON OBJECT::dbo.DimStore
	TO test_user;

GRANT SELECT ON OBJECT::dbo.FLiquorSales
	TO test_user;
GRANT INSERT ON OBJECT::dbo.FLiquorSales
	TO test_user;
GRANT ALTER ON OBJECT::dbo.FLiquorSales
	TO test_user;
GRANT DELETE ON OBJECT::dbo.FLiquorSales
	TO test_user;
GRANT UPDATE ON OBJECT::dbo.FLiquorSales
	TO test_user;


COMMIT TRAN create_test_user;