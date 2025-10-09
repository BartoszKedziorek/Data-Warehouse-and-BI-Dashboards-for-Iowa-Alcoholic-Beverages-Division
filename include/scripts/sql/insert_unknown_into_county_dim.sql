SET IDENTITY_INSERT [Iowa_Sales_Data_Warehouse].dbo.DimCounty ON;
                           
INSERT INTO [Iowa_Sales_Data_Warehouse].dbo.DimCounty(CountyId, CountyName, CountyNumber)
VALUES(-1, 'unknown', 'unknown');
                                      
SET IDENTITY_INSERT [Iowa_Sales_Data_Warehouse].dbo.DimCounty OFF;