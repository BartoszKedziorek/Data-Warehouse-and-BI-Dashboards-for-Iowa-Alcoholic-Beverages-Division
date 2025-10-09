SET IDENTITY_INSERT [Iowa_Sales_Data_Warehouse].dbo.DimVendor ON;
                           
INSERT INTO [Iowa_Sales_Data_Warehouse].dbo.DimVendor(VendorId, VendorName, VendorNumberDK,
                                                    StartDate, EndDate, IsCurrent)
VALUES(-1, 'unknown', -1,
        DATEFROMPARTS(1900, 1, 1), NULL, CAST(1 as bit));
                                      
SET IDENTITY_INSERT [Iowa_Sales_Data_Warehouse].dbo.DimVendor OFF;