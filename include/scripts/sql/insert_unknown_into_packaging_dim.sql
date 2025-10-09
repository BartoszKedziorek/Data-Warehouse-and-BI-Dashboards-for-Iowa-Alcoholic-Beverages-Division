SET IDENTITY_INSERT [Iowa_Sales_Data_Warehouse].dbo.DimPackaging ON;
                           
INSERT INTO [Iowa_Sales_Data_Warehouse].dbo.DimPackaging(PackagingId, NumberOfBottlesInPack, BottleVolumeML)
VALUES(-1, -1, -1);
                                      
SET IDENTITY_INSERT [Iowa_Sales_Data_Warehouse].dbo.DimPackaging OFF;