SET IDENTITY_INSERT [Iowa_Sales_Data_Warehouse].dbo.DimItem ON;
                           
INSERT INTO [Iowa_Sales_Data_Warehouse].dbo.DimItem(ItemId, ItemNumberDK, ItemName,
                                                CategoryNumberDK, CategoryName, StartDate,
                                                EndDate, IsCurrent)
VALUES(-1, -1, 'unknown',
        -1, 'unknown', DATEFROMPARTS(1900, 1, 1),
        NULL, CAST(1 as bit));
                                      
SET IDENTITY_INSERT [Iowa_Sales_Data_Warehouse].dbo.DimItem OFF;