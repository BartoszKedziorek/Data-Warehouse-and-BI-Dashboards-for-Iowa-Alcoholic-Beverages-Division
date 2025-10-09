SET IDENTITY_INSERT [Iowa_Sales_Data_Warehouse].dbo.DimStore ON;
                           
INSERT INTO [Iowa_Sales_Data_Warehouse].dbo.DimStore(StoreId, StoreNumberDK, StoreName,
                                            [Address], City, ZipCode,
                                            StoreLocation, StartDate, EndDate,
                                            IsCurrent, StoreLocationLongitude,
                                            StoreLocationLatitude)
VALUES(-1, -1, 'unknown',
        'unknown', 'unknown', -1,
        geography::STGeomFromText('POINT EMPTY', 4326), DATEFROMPARTS(1900, 1, 1), NULL,
        CAST(1 as bit), -1.0, -1.0);
                                      
SET IDENTITY_INSERT [Iowa_Sales_Data_Warehouse].dbo.DimStore OFF;