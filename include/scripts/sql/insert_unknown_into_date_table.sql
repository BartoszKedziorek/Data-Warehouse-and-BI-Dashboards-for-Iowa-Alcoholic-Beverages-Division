SET IDENTITY_INSERT [Iowa_Sales_Data_Warehouse].dbo.DimDateTable ON;
                           
INSERT INTO [Iowa_Sales_Data_Warehouse].dbo.DimDateTable(DateId, FullDate, DayOfYearNumber,
                 DayOfMonthNumber, DayOfWeekNumber, DayOfWeekName,
                 IsWeekend, AstronomicalSeasonNumber,AstronomicalSeasonName,
                 MonthNumber, MonthLongName, MonthShortName,
                 Year, YearMonth)
VALUES(-1, DATEFROMPARTS(1900, 1, 1), -1,
        -1, -1, 'unknown',
        CAST(0 as bit), -1, 'unknown',
        -1, 'unknown', 'unknown',
        -1, 'unknown');
                                      
SET IDENTITY_INSERT [Iowa_Sales_Data_Warehouse].dbo.DimDateTable OFF;