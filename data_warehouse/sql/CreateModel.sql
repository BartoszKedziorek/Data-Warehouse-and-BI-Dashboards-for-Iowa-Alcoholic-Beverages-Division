BEGIN TRAN create_model;

CREATE TABLE DimDateTable(
	DateId SMALLINT IDENTITY(1, 1) NOT NULL,
	FullDate DATE NOT NULL,
	DayOfYearNumber SMALLINT NOT NULL,
	DayOfMonthNumber SMALLINT NOT NULL,
	DayOfWeekNumber SMALLINT NOT NULL,
	DayOfWeekName VARCHAR(9) NOT NULL,
	IsWeekend BIT NOT NULL,
	AstronomicalSeasonNumber SMALLINT NOT NULL,
	AstronomicalSeasonName CHAR(7) NOT NULL,
	MonthNumber SMALLINT NOT NULL,
	MonthLongName VARCHAR(9) NOT NULL,
	MonthShortName VARCHAR(7) NOT NULL,
	[Year] SMALLINT  NOT NULL,
	YearMonth VARCHAR(7) NOT NULL,
	CONSTRAINT PK_DimDateTable_DateId PRIMARY KEY CLUSTERED(DateId),
	CONSTRAINT UQ_FullDate UNIQUE(FullDate),
	CONSTRAINT CHECK_DayOfYearNumberBetween1and366 CHECK ( DayOfYearNumber BETWEEN 1 AND 366 OR DayOfYearNumber = -1),
	CONSTRAINT CHECK_DayOfMonthNumberIsValid CHECK (
		CAST(
			CASE
				WHEN MonthNumber IN (1, 3, 5, 7, 8, 10, 12)
					AND DayOfMonthNumber BETWEEN 1 AND 31
					THEN 1
				WHEN MonthNumber IN (4, 6, 9, 11)
					AND DayOfMonthNumber BETWEEN 1 AND 30
					THEN 1
				WHEN [Year] % 4 != 0 AND MonthNumber = 2
					AND DayOfMonthNumber BETWEEN 1 AND 28
					THEN 1
				WHEN [Year] % 4 = 0 AND MonthNumber = 2
					AND DayOfMonthNumber BETWEEN 1 AND 29
					THEN 1
				WHEN MonthNumber = -1
					THEN 1
				ELSE 0
			END 
		AS BIT) = 1
	),
	CONSTRAINT CHECK_DayOfWeekNumberBetween1and7 CHECK (DayOfWeekNumber BETWEEN 1 AND 7 OR DayOfWeekNumber = -1),
	CONSTRAINT CHECK_AstronomicalSeasonNumberBetween1and4 CHECK (AstronomicalSeasonNumber BETWEEN 1 AND 4 OR AstronomicalSeasonNumber = -1),
	CONSTRAINT CHECK_MonthNumberBetween1and12 CHECK (MonthNumber BETWEEN 1 AND 12 OR MonthNumber = -1)
);

CREATE TABLE DimStore(
	StoreId INT IDENTITY(1, 1) NOT NULL,
	StoreNumberDK SMALLINT NOT NULL,
	StoreName VARCHAR(65) NOT NULL,
	[Address] VARCHAR(65) NOT NULL,
	City VARCHAR(22) NOT NULL,
	ZipCode INT NOT NULL,
	StoreLocation GEOGRAPHY NOT NULL,
	StartDate DATE NOT NULL,
	EndDate DATE NULL,
	IsCurrent BIT NOT NULL,
	CONSTRAINT PK_DimStore_StoreId PRIMARY KEY CLUSTERED(StoreId),
	CONSTRAINT CHECK_StoreLocationIsValid CHECK(
			(StoreLocation.Lat BETWEEN -90 AND 90)
			AND
			(StoreLocation.Long BETWEEN -180 AND 180)
	)
);

ALTER TABLE dbo.DimStore
ADD StoreLocationLongitude DECIMAL(19,15) NOT NULL;
ALTER TABLE dbo.DimStore
ADD StoreLocationLatitude DECIMAL(17,15) NOT NULL;

ALTER TABLE dbo.DimStore
ADD CONSTRAINT UQ_DimStoreSCD2 UNIQUE (StoreNumberDK, StoreName,
									[Address], City, ZipCode, StartDate,
									StoreLocationLongitude, StoreLocationLatitude)

CREATE TABLE DimCounty(
	CountyId SMALLINT IDENTITY(1, 1) NOT NULL,
	CountyName VARCHAR(15) NOT NULL,
	CountyNumber VARCHAR(7) NOT NULL,
	CONSTRAINT PK_DimCounty_CountyId PRIMARY KEY CLUSTERED(CountyId),
	CONSTRAINT UQ_CountyName UNIQUE(CountyName),
	CONSTRAINT UQ_CountyNumber UNIQUE(CountyNumber)
);

CREATE TABLE DimVendor(
	VendorId SMALLINT IDENTITY(1, 1) NOT NULL,
	VendorName VARCHAR(75) NOT NULL,
	VendorNumberDK smallint NOT NULL,
	StartDate DATE NOT NULL,
	EndDate DATE NULL,
	IsCurrent BIT NOT NULL,
	CONSTRAINT PK_DimVendor_VendorId PRIMARY KEY CLUSTERED(VendorId),
	CONSTRAINT UQ_DimVendorSCD2 UNIQUE (VendorName, VendorNumberDK, StartDate)
);

CREATE TABLE DimItem(
	ItemId INT IDENTITY(1, 1) NOT NULL,
	ItemNumberDK INT NOT NULL,
	ItemName VARCHAR(75) NOT NULL,
	CategoryNumberDK INT NOT NULL,
	CategoryName VARCHAR(45) NOT NULL,
	StartDate DATE NOT NULL,
	EndDate DATE NULL,
	IsCurrent BIT NOT NULL,
	CONSTRAINT PK_DimItem_ItemId PRIMARY KEY CLUSTERED(ItemId),
	CONSTRAINT UQ_DimItemSCD2 UNIQUE (ItemNumberDK, ItemName, CategoryNumberDK,
									 CategoryName, StartDate)
);

CREATE TABLE DimPackaging(
	PackagingId SMALLINT IDENTITY(1, 1) NOT NULL,
	NumberOfBottlesInPack SMALLINT NOT NULL,
	BottleVolumeML INT NOT NULL,
	CONSTRAINT PK_DimPackaging_PackagingId PRIMARY KEY CLUSTERED(PackagingId),
	CONSTRAINT UQ_NumberOfBottlesInPack_BottleVolumeML UNIQUE (NumberOfBottlesInPack, BottleVolumeML),
);

CREATE TABLE FLiquorSales(
	LiquorSalesId INT IDENTITY(1, 1) NOT NULL,
	BottlesSold INT NOT NULL,
	VolumeSoldLiters DECIMAL(8, 2) NOT NULL,
	StateBottleCostUSD DECIMAL(8, 2) NOT NULL,
	StateBottleRetailUSD DECIMAL(8, 2) NOT NULL,
	RevenueUSD DECIMAL(9, 2) NOT NULL,
	TotalCostUSD DECIMAL(9, 2) NOT NULL,
	GrossProfitUSD DECIMAL(9, 2) NOT NULL,
	GrossProfitMargin DECIMAL(5, 2) NOT NULL,
	InvoiceNumber VARCHAR(13) NOT NULL,
	DateId SMALLINT NOT NULL,
	StoreId INT NOT NULL,
	CountyId SMALLINT NOT NULL,
	VendorId SMALLINT NOT NULL,
	ItemId INT NOT NULL,
	PackagingId SMALLINT NOT NULL,
	CONSTRAINT PK_FLiquorSales_LiquorSalesId PRIMARY KEY NONCLUSTERED(LiquorSalesId),
	/*Foreign keys*/
	CONSTRAINT FK_FLiquorSales_DimDateTable FOREIGN KEY (DateId)
		REFERENCES DimDateTable(DateId),
	CONSTRAINT FK_FLiquorSales_DimStore FOREIGN KEY (StoreId)
		REFERENCES DimStore(StoreId),
	CONSTRAINT FK_FLiquorSales_DimCounty FOREIGN KEY (CountyId)
		REFERENCES DimCounty(CountyId),
	CONSTRAINT FK_FLiquorSales_DimVendor FOREIGN KEY (VendorId)
		REFERENCES DimVendor(VendorId),
	CONSTRAINT FK_FLiquorSales_DimItem FOREIGN KEY (ItemId)
		REFERENCES DimItem(ItemId),
	CONSTRAINT FK_FLiquorSales_DimPackaging FOREIGN KEY (PackagingId)
		REFERENCES DimPackaging(PackagingId) 
);

CREATE CLUSTERED COLUMNSTORE INDEX CSI_FLiquorSales ON FLiquorSales;


COMMIT TRAN create_model;