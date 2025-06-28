# Snowflake-BI-Data-Warehousing
### Description: 
A retail focused Business Intelligence and Data Warehousing project built using Snowflake. The system applies dimensional modeling with surrogate keys, star schema architecture, and secure SQL views to support scalable analytics and data visualizations. It includes complete dimension and fact tables, unknown member handling, SQL pass through views, and a curated data access layer designed for Tableau integration.

## Data Warehouse Design: Star Schema
![model_answer_key](https://github.com/user-attachments/assets/c5e2fb86-eb2b-43e3-ab63-124aac2a2837)

## First Step: Develop ELT and Staging in Snowflake
Created a Snowflake database to load and stage raw data from Azure Blob Storage. While tables could be generated using the Snowflake wizard, I manually created them using SQL for practice and then loaded the data into each staging table.

```
CREATE DATABASE my_database;
CREATE OR REPLACE TABLE STAGING_CHANNELCATEGORY (
    ChannelCategoryID INT PRIMARY KEY,
    ChannelCategory VARCHAR(255),
    CreatedDate VARCHAR(255),
    CreatedBy VARCHAR(255),
    ModifiedDate VARCHAR(255),
    ModifiedBy VARCHAR(255)
);

CREATE OR REPLACE TABLE STAGING_CHANNEL (
    ChannelID INT PRIMARY KEY,
    ChannelCategoryID INT,
    Channel VARCHAR(255),
    CreatedDate DATETIME,
    CreatedBy VARCHAR(255),
    ModifiedDate DATETIME,
    ModifiedBy VARCHAR(255),
    FOREIGN KEY (ChannelCategoryID) REFERENCES STAGING_CHANNELCATEGORY(ChannelCategoryID)
);

CREATE OR REPLACE TABLE STAGING_SEGMENT (
    SegmentID INT PRIMARY KEY,
    Segment VARCHAR(255),
    CreatedDate VARCHAR(255),
    CreatedBy VARCHAR(255),
    ModifiedDate VARCHAR(255),
    ModifiedBy VARCHAR(255)
);

CREATE OR REPLACE TABLE STAGING_SUBSEGMENT (
    SubSegmentID INT PRIMARY KEY,
    SegmentID INT,
    SubSegment VARCHAR(255),
    CreatedDate VARCHAR(255),
    CreatedBy VARCHAR(255),
    ModifiedDate VARCHAR(255),
    ModifiedBy VARCHAR(255),
    FOREIGN KEY (SegmentID) REFERENCES STAGING_SEGMENT(SegmentID)
);

CREATE OR REPLACE TABLE STAGING_RESELLER (
    ResellerID VARCHAR(255) PRIMARY KEY,  
    Contact VARCHAR(255),
    EmailAddress VARCHAR(255),
    Address VARCHAR(255),
    City VARCHAR(255),
    StateProvince VARCHAR(255),
    Country VARCHAR(255),
    PostalCode VARCHAR(255),
    PhoneNumber VARCHAR(255),
    CreatedDate VARCHAR(255),
    CreatedBy VARCHAR(255),
    ModifiedDate VARCHAR(255),
    ModifiedBy VARCHAR(255),
    ResellerName VARCHAR(255)
);

CREATE OR REPLACE TABLE STAGING_STORE (
    StoreID INT PRIMARY KEY,
    SubSegmentID INT,
    StoreNumber INT,
    StoreManager VARCHAR(255),
    Address VARCHAR(255),
    City VARCHAR(255),
    StateProvince VARCHAR(255),
    Country VARCHAR(255),
    PostalCode VARCHAR(255),
    PhoneNumber VARCHAR(255),
    CreatedDate VARCHAR(255),
    CreatedBy VARCHAR(255),
    ModifiedDate VARCHAR(255),
    ModifiedBy VARCHAR(255),
    FOREIGN KEY (SubSegmentID) REFERENCES STAGING_SUBSEGMENT(SubSegmentID)
);

CREATE OR REPLACE TABLE STAGING_CUSTOMER (
    CustomerID VARCHAR(255) PRIMARY KEY,
    SubSegmentID INT,
    FirstName VARCHAR(255),
    LastName VARCHAR(255),
    Gender VARCHAR(50),
    EmailAddress VARCHAR(255),
    Address VARCHAR(255),
    City VARCHAR(255),
    StateProvince VARCHAR(255),
    Country VARCHAR(255),
    PostalCode VARCHAR(255),
    PhoneNumber VARCHAR(255),
    CreatedDate VARCHAR(255),
    CreatedBy VARCHAR(255),
    ModifiedDate VARCHAR(255),
    ModifiedBy VARCHAR(255),
    FOREIGN KEY (SubSegmentID) REFERENCES STAGING_SUBSEGMENT(SubSegmentID)
);

CREATE OR REPLACE TABLE STAGING_SALESHEADER (
    SalesHeaderID INT PRIMARY KEY,
    Date DATE,
    ChannelID INT,
    StoreID INT,
    CustomerID VARCHAR(255),
    ResellerID VARCHAR(255),
    CreatedDate VARCHAR(255),
    CreatedBy VARCHAR(255),
    ModifiedDate VARCHAR(255),
    ModifiedBy VARCHAR(255),
    FOREIGN KEY (ChannelID) REFERENCES STAGING_CHANNEL(ChannelID),
    FOREIGN KEY (StoreID) REFERENCES STAGING_STORE(StoreID),
    FOREIGN KEY (CustomerID) REFERENCES STAGING_CUSTOMER(CustomerID),
    FOREIGN KEY (ResellerID) REFERENCES STAGING_RESELLER(ResellerID)
);

CREATE OR REPLACE TABLE STAGING_PRODUCTCATEGORY (
    ProductCategoryID INT PRIMARY KEY,
    ProductCategory VARCHAR(255),
    CreatedDate VARCHAR(255),
    CreatedBy VARCHAR(255),
    ModifiedDate VARCHAR(255),
    ModifiedBy VARCHAR(255)
);

CREATE OR REPLACE TABLE STAGING_PRODUCTTYPE (
    ProductTypeID INT PRIMARY KEY,
    ProductCategoryID INT,
    ProductType VARCHAR(255),
    CreatedDate VARCHAR(255),
    CreatedBy VARCHAR(255),
    ModifiedDate VARCHAR(255),
    ModifiedBy VARCHAR(255),
    FOREIGN KEY (ProductCategoryID) REFERENCES STAGING_PRODUCTCATEGORY(ProductCategoryID)
);

CREATE OR REPLACE TABLE STAGING_UNITOFMEASURE (
    UnitOfMeasureID INT PRIMARY KEY,
    UnitOfMeasure VARCHAR(255),
    CreatedDate VARCHAR(255),
    CreatedBy VARCHAR(255),
    ModifiedDate VARCHAR(255),
    ModifiedBy VARCHAR(255)
);

CREATE OR REPLACE TABLE STAGING_PRODUCT (
    ProductID INT PRIMARY KEY,
    ProductTypeID INT,
    Product VARCHAR(255),
    Color VARCHAR(255),
    Style VARCHAR(255),
    UnitofMeasureID INT,
    Weight FLOAT,
    Price FLOAT,
    Cost FLOAT,
    CreatedDate VARCHAR(255),
    CreatedBy VARCHAR(255),
    ModifiedDate VARCHAR(255),
    ModifiedBy VARCHAR(255),
    WholesalePrice FLOAT,
    FOREIGN KEY (ProductTypeID) REFERENCES STAGING_PRODUCTTYPE(ProductTypeID),
    FOREIGN KEY (UnitofMeasureID) REFERENCES STAGING_UNITOFMEASURE(UnitOfMeasureID)
);


CREATE OR REPLACE TABLE STAGING_SALESDETAIL (
    SalesDetailID INT PRIMARY KEY,
    SalesHeaderID INT,
    ProductID INT,
    SalesQuantity INT,
    SalesAmount FLOAT,
    CreatedDate VARCHAR(255),
    CreatedBy VARCHAR(255),
    ModifiedDate VARCHAR(255),
    ModifiedBy VARCHAR(255),
    FOREIGN KEY (SalesHeaderID) REFERENCES STAGING_SALESHEADER(SalesHeaderID),
    FOREIGN KEY (ProductID) REFERENCES STAGING_PRODUCT(ProductID)
);

CREATE OR REPLACE TABLE STAGING_TARGET_CHANNEL_RESELLER_STORE (
    Year INT,
    ChannelName VARCHAR(255),
    TargetName VARCHAR(255),
    TargetSalesAmount FLOAT
);

CREATE OR REPLACE TABLE STAGING_TARGET_PRODUCT (
    ProductID INT,
    Product VARCHAR(255),
    Year INT,
    SalesQuantityTarget INT
);


--If we are not changing the data type of the date column to VARCHAR, then we can use SQL instead
COPY INTO STAGING_CHANNEL
FROM @CHANNEL/Channel.csv
FILE_FORMAT = my_csv_format;

--No Segment table in the blobstore, therefore we create an internal stage. 
--After that, upload local file to the Stage via Snowflake UI
CREATE OR REPLACE STAGE SEGMENT;


--All the data has been successfully loaded. Below are three SELECT queries to test it out.

--1.Find which color is more popular.
SELECT p.color, SUM(sd.salesquantity) AS ttl_quantity_sold
FROM STAGING_PRODUCT p
JOIN STAGING_SALESDETAIL sd ON p.productid = sd.productid
GROUP BY p.color
ORDER BY ttl_quantity_sold DESC

--2.Find the accumulated total number of orders placed by each individual customer
SELECT c.firstname, c.lastname, COUNT(*)
FROM STAGING_CUSTOMER c
JOIN STAGING_SALESHEADER sh ON c.customerid = sh.customerid
JOIN STAGING_SALESDETAIL sd ON sh.salesheaderid = sd.salesheaderid
GROUP BY c.firstname, c.lastname

--3. Effectiveness of management team.
SELECT s.storeid, s.storenumber, s.storemanager, COUNT(sh.salesheaderid) AS ttl_transactions
FROM STAGING_STORE s
JOIN STAGING_SALESHEADER sh ON s.storeid = sh.storeid
GROUP BY s.storeid, s.storenumber, s.storemanager
ORDER BY ttl_transactions DESC
```
## Second Step: Create and Load Dimension Tables in Snowflake
In this step, dimension tables were created to provide the descriptive context needed for analyzing facts in a data warehouse. These tables include attributes such as names, categories, and locations, which support filtering, grouping, and detailed analysis. Each table was built using SQL, with surrogate keys, clearly defined grain, and proper relationships to fact tables.

```
--First, create table DIM_DATE
CREATE OR REPLACE table DIM_DATE (
	DATE_PKEY				number(9) PRIMARY KEY,
	DATE					date not null,
	FULL_DATE_DESC			varchar(64) not null,
	DAY_NUM_IN_WEEK			number(1) not null,
	DAY_NUM_IN_MONTH		number(2) not null,
	DAY_NUM_IN_YEAR			number(3) not null,
	DAY_NAME				varchar(10) not null,
	DAY_ABBREV				varchar(3) not null,
	WEEKDAY_IND				varchar(64) not null,
	US_HOLIDAY_IND			varchar(64) not null,
	/*<COMPANYNAME>*/_HOLIDAY_IND varchar(64) not null,
	MONTH_END_IND			varchar(64) not null,
	WEEK_BEGIN_DATE_NKEY		number(9) not null,
	WEEK_BEGIN_DATE			date not null,
	WEEK_END_DATE_NKEY		number(9) not null,
	WEEK_END_DATE			date not null,
	WEEK_NUM_IN_YEAR		number(9) not null,
	MONTH_NAME				varchar(10) not null,
	MONTH_ABBREV			varchar(3) not null,
	MONTH_NUM_IN_YEAR		number(2) not null,
	YEARMONTH				varchar(10) not null,
	QUARTER					number(1) not null,
	YEARQUARTER				varchar(10) not null,
	YEAR					number(5) not null,
	FISCAL_WEEK_NUM			number(2) not null,
	FISCAL_MONTH_NUM		number(2) not null,
	FISCAL_YEARMONTH		varchar(10) not null,
	FISCAL_QUARTER			number(1) not null,
	FISCAL_YEARQUARTER		varchar(10) not null,
	FISCAL_HALFYEAR			number(1) not null,
	FISCAL_YEAR				number(5) not null,
	SQL_TIMESTAMP			timestamp_ntz,
	CURRENT_ROW_IND			char(1) default 'Y',
	EFFECTIVE_DATE			date default to_date(current_timestamp),
	EXPIRATION_DATE			date default To_date('9999-12-31') 
)
comment = 'Type 0 Dimension Table Housing Calendar and Fiscal Year Date Attributes'; 

-- Populate data into DIM_DATE
insert into DIM_DATE
select DATE_PKEY,
		DATE_COLUMN,
        FULL_DATE_DESC,
		DAY_NUM_IN_WEEK,
		DAY_NUM_IN_MONTH,
		DAY_NUM_IN_YEAR,
		DAY_NAME,
		DAY_ABBREV,
		WEEKDAY_IND,
		US_HOLIDAY_IND,
        COMPANY_HOLIDAY_IND,
		MONTH_END_IND,
		WEEK_BEGIN_DATE_NKEY,
		WEEK_BEGIN_DATE,
		WEEK_END_DATE_NKEY,
		WEEK_END_DATE,
		WEEK_NUM_IN_YEAR,
		MONTH_NAME,
		MONTH_ABBREV,
		MONTH_NUM_IN_YEAR,
		YEARMONTH,
		CURRENT_QUARTER,
		YEARQUARTER,
		CURRENT_YEAR,
		FISCAL_WEEK_NUM,
		FISCAL_MONTH_NUM,
		FISCAL_YEARMONTH,
		FISCAL_QUARTER,
		FISCAL_YEARQUARTER,
		FISCAL_HALFYEAR,
		FISCAL_YEAR,
		SQL_TIMESTAMP,
		CURRENT_ROW_IND,
		EFFECTIVE_DATE,
		EXPIRA_DATE
	from 
	    
        
    --( select to_date('01-25-2019 23:25:11.120','MM-DD-YYYY HH24:MI:SS.FF') as DD, /*<<Modify date for preferred table start date*/    
    --( select to_date('2013-01-01 00:00:01','YYYY-MM-DD HH24:MI:SS') as DD, /*<<Modify date for preferred table start date*/
	  ( select to_date('2012-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS') as DD, /*<<Modify date for preferred table start date*/
			seq1() as Sl,row_number() over (order by Sl) as row_numbers,
			dateadd(day,row_numbers,DD) as V_DATE,
			case when date_part(dd, V_DATE) < 10 and date_part(mm, V_DATE) > 9 then
				date_part(year, V_DATE)||date_part(mm, V_DATE)||'0'||date_part(dd, V_DATE)
				 when date_part(dd, V_DATE) < 10 and  date_part(mm, V_DATE) < 10 then 
				 date_part(year, V_DATE)||'0'||date_part(mm, V_DATE)||'0'||date_part(dd, V_DATE)
				 when date_part(dd, V_DATE) > 9 and  date_part(mm, V_DATE) < 10 then
				 date_part(year, V_DATE)||'0'||date_part(mm, V_DATE)||date_part(dd, V_DATE)
				 when date_part(dd, V_DATE) > 9 and  date_part(mm, V_DATE) > 9 then
				 date_part(year, V_DATE)||date_part(mm, V_DATE)||date_part(dd, V_DATE) end as DATE_PKEY,
			V_DATE as DATE_COLUMN,
			dayname(dateadd(day,row_numbers,DD)) as DAY_NAME_1,
			case 
				when dayname(dateadd(day,row_numbers,DD)) = 'Mon' then 'Monday'
				when dayname(dateadd(day,row_numbers,DD)) = 'Tue' then 'Tuesday'
				when dayname(dateadd(day,row_numbers,DD)) = 'Wed' then 'Wednesday'
				when dayname(dateadd(day,row_numbers,DD)) = 'Thu' then 'Thursday'
				when dayname(dateadd(day,row_numbers,DD)) = 'Fri' then 'Friday'
				when dayname(dateadd(day,row_numbers,DD)) = 'Sat' then 'Saturday'
				when dayname(dateadd(day,row_numbers,DD)) = 'Sun' then 'Sunday' end ||', '||
			case when monthname(dateadd(day,row_numbers,DD)) ='Jan' then 'January'
				   when monthname(dateadd(day,row_numbers,DD)) ='Feb' then 'February'
				   when monthname(dateadd(day,row_numbers,DD)) ='Mar' then 'March'
				   when monthname(dateadd(day,row_numbers,DD)) ='Apr' then 'April'
				   when monthname(dateadd(day,row_numbers,DD)) ='May' then 'May'
				   when monthname(dateadd(day,row_numbers,DD)) ='Jun' then 'June'
				   when monthname(dateadd(day,row_numbers,DD)) ='Jul' then 'July'
				   when monthname(dateadd(day,row_numbers,DD)) ='Aug' then 'August'
				   when monthname(dateadd(day,row_numbers,DD)) ='Sep' then 'September'
				   when monthname(dateadd(day,row_numbers,DD)) ='Oct' then 'October'
				   when monthname(dateadd(day,row_numbers,DD)) ='Nov' then 'November'
				   when monthname(dateadd(day,row_numbers,DD)) ='Dec' then 'December' end
				   ||' '|| to_varchar(dateadd(day,row_numbers,DD), ' dd, yyyy') as FULL_DATE_DESC,
			dateadd(day,row_numbers,DD) as V_DATE_1,
			dayofweek(V_DATE_1)+1 as DAY_NUM_IN_WEEK,
			Date_part(dd,V_DATE_1) as DAY_NUM_IN_MONTH,
			dayofyear(V_DATE_1) as DAY_NUM_IN_YEAR,
			case 
				when dayname(V_DATE_1) = 'Mon' then 'Monday'
				when dayname(V_DATE_1) = 'Tue' then 'Tuesday'
				when dayname(V_DATE_1) = 'Wed' then 'Wednesday'
				when dayname(V_DATE_1) = 'Thu' then 'Thursday'
				when dayname(V_DATE_1) = 'Fri' then 'Friday'
				when dayname(V_DATE_1) = 'Sat' then 'Saturday'
				when dayname(V_DATE_1) = 'Sun' then 'Sunday' end as	DAY_NAME,
			dayname(dateadd(day,row_numbers,DD)) as DAY_ABBREV,
			case  
				when dayname(V_DATE_1) = 'Sun' and dayname(V_DATE_1) = 'Sat' then 
                 'Not-Weekday'
				else 'Weekday' end as WEEKDAY_IND,
			 case 
				when (DATE_PKEY = date_part(year, V_DATE)||'0101' or DATE_PKEY = date_part(year, V_DATE)||'0704' or
				DATE_PKEY = date_part(year, V_DATE)||'1225' or DATE_PKEY = date_part(year, V_DATE)||'1226') then  
				'Holiday' 
				when monthname(V_DATE_1) ='May' and dayname(last_day(V_DATE_1)) = 'Wed' 
				and dateadd(day,-2,last_day(V_DATE_1)) = V_DATE_1  then
				'Holiday'
				when monthname(V_DATE_1) ='May' and dayname(last_day(V_DATE_1)) = 'Thu' 
				and dateadd(day,-3,last_day(V_DATE_1)) = V_DATE_1  then
				'Holiday'
				when monthname(V_DATE_1) ='May' and dayname(last_day(V_DATE_1)) = 'Fri' 
				and dateadd(day,-4,last_day(V_DATE_1)) = V_DATE_1 then
				'Holiday'
				when monthname(V_DATE_1) ='May' and dayname(last_day(V_DATE_1)) = 'Sat' 
				and dateadd(day,-5,last_day(V_DATE_1)) = V_DATE_1  then
				'Holiday'
				when monthname(V_DATE_1) ='May' and dayname(last_day(V_DATE_1)) = 'Sun' 
				and dateadd(day,-6,last_day(V_DATE_1)) = V_DATE_1 then
				'Holiday'
				when monthname(V_DATE_1) ='May' and dayname(last_day(V_DATE_1)) = 'Mon' 
				and last_day(V_DATE_1) = V_DATE_1 then
				'Holiday'
				when monthname(V_DATE_1) ='May' and dayname(last_day(V_DATE_1)) = 'Tue' 
				and dateadd(day,-1 ,last_day(V_DATE_1)) = V_DATE_1  then
				'Holiday'
				when monthname(V_DATE_1) ='Sep' and dayname(date_part(year, V_DATE_1)||'-09-01') = 'Wed' 
				and dateadd(day,5,(date_part(year, V_DATE_1)||'-09-01')) = V_DATE_1  then
				'Holiday' 
				when monthname(V_DATE_1) ='Sep' and dayname(date_part(year, V_DATE_1)||'-09-01') = 'Thu' 
				and dateadd(day,4,(date_part(year, V_DATE_1)||'-09-01')) = V_DATE_1  then
				'Holiday' 
				when monthname(V_DATE_1) ='Sep' and dayname(date_part(year, V_DATE_1)||'-09-01') = 'Fri' 
				and dateadd(day,3,(date_part(year, V_DATE_1)||'-09-01')) = V_DATE_1 then
				'Holiday' 
				when monthname(V_DATE_1) ='Sep' and dayname(date_part(year, V_DATE_1)||'-09-01') = 'Sat' 
				and dateadd(day,2,(date_part(year, V_DATE_1)||'-09-01')) = V_DATE_1  then
				'Holiday' 
				when monthname(V_DATE_1) ='Sep' and dayname(date_part(year, V_DATE_1)||'-09-01') = 'Sun' 
				and dateadd(day,1,(date_part(year, V_DATE_1)||'-09-01')) = V_DATE_1 then
				'Holiday' 
				when monthname(V_DATE_1) ='Sep' and dayname(date_part(year, V_DATE_1)||'-09-01') = 'Mon' 
				and date_part(year, V_DATE_1)||'-09-01' = V_DATE_1 then
				'Holiday' 
				when monthname(V_DATE_1) ='Sep' and dayname(date_part(year, V_DATE_1)||'-09-01') = 'Tue' 
				and dateadd(day,6 ,(date_part(year, V_DATE_1)||'-09-01')) = V_DATE_1  then
				'Holiday' 
				when monthname(V_DATE_1) ='Nov' and dayname(date_part(year, V_DATE_1)||'-11-01') = 'Wed' 
				and (dateadd(day,23,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1  or 
					 dateadd(day,22,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1 ) then
				'Holiday'
				when monthname(V_DATE_1) ='Nov' and dayname(date_part(year, V_DATE_1)||'-11-01') = 'Thu' 
				and ( dateadd(day,22,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1 or 
					 dateadd(day,21,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1 ) then
				'Holiday'
				when monthname(V_DATE_1) ='Nov' and dayname(date_part(year, V_DATE_1)||'-11-01') = 'Fri' 
				and ( dateadd(day,21,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1 or 
					 dateadd(day,20,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1 ) then
				 'Holiday'
				when monthname(V_DATE_1) ='Nov' and dayname(date_part(year, V_DATE_1)||'-11-01') = 'Sat' 
				and ( dateadd(day,27,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1 or 
					 dateadd(day,26,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1 ) then
				'Holiday'
				when monthname(V_DATE_1) ='Nov' and dayname(date_part(year, V_DATE_1)||'-11-01') = 'Sun' 
				and ( dateadd(day,26,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1 or 
					 dateadd(day,25,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1 ) then
				'Holiday'
				when monthname(V_DATE_1) ='Nov' and dayname(date_part(year, V_DATE_1)||'-11-01') = 'Mon' 
				and (dateadd(day,25,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1 or 
					 dateadd(day,24,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1 ) then
				'Holiday'
				when monthname(V_DATE_1) ='Nov' and dayname(date_part(year, V_DATE_1)||'-11-01') = 'Tue' 
				and (dateadd(day,24,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1 or 
					 dateadd(day,23,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1 ) then
				 'Holiday'    
				else
				'Not-Holiday' end as US_HOLIDAY_IND,
			/*Modify the following for Company Specific Holidays*/
			case 
				when (DATE_PKEY = date_part(year, V_DATE)||'0101' or DATE_PKEY = date_part(year, V_DATE)||'0219'
				or DATE_PKEY = date_part(year, V_DATE)||'0528' or DATE_PKEY = date_part(year, V_DATE)||'0704' 
				or DATE_PKEY = date_part(year, V_DATE)||'1225' )then 
				'Holiday'               
                when monthname(V_DATE_1) ='Mar' and dayname(last_day(V_DATE_1)) = 'Fri' 
				and last_day(V_DATE_1) = V_DATE_1 then
				'Holiday'
				when monthname(V_DATE_1) ='Mar' and dayname(last_day(V_DATE_1)) = 'Sat' 
				and dateadd(day,-1,last_day(V_DATE_1)) = V_DATE_1  then
				'Holiday'
				when monthname(V_DATE_1) ='Mar' and dayname(last_day(V_DATE_1)) = 'Sun' 
				and dateadd(day,-2,last_day(V_DATE_1)) = V_DATE_1 then
				'Holiday'
				when monthname(V_DATE_1) ='Apr' and dayname(date_part(year, V_DATE_1)||'-04-01') = 'Tue'
                and dateadd(day,3,(date_part(year, V_DATE_1)||'-04-01')) = V_DATE_1 then
				'Holiday'
				when monthname(V_DATE_1) ='Apr' and dayname(date_part(year, V_DATE_1)||'-04-01') = 'Wed' 
				and dateadd(day,2,(date_part(year, V_DATE_1)||'-04-01')) = V_DATE_1 then
				'Holiday'
				when monthname(V_DATE_1) ='Apr' and dayname(date_part(year, V_DATE_1)||'-04-01') = 'Thu'
                and dateadd(day,1,(date_part(year, V_DATE_1)||'-04-01')) = V_DATE_1 then
				'Holiday'
				when monthname(V_DATE_1) ='Apr' and dayname(date_part(year, V_DATE_1)||'-04-01') = 'Fri' 
				and date_part(year, V_DATE_1)||'-04-01' = V_DATE_1 then
				'Holiday'
                when monthname(V_DATE_1) ='Apr' and dayname(date_part(year, V_DATE_1)||'-04-01') = 'Wed' 
				and dateadd(day,5,(date_part(year, V_DATE_1)||'-04-01')) = V_DATE_1  then
				'Holiday' 
				when monthname(V_DATE_1) ='Apr' and dayname(date_part(year, V_DATE_1)||'-04-01') = 'Thu' 
				and dateadd(day,4,(date_part(year, V_DATE_1)||'-04-01')) = V_DATE_1  then
				'Holiday' 
				when monthname(V_DATE_1) ='Apr' and dayname(date_part(year, V_DATE_1)||'-04-01') = 'Fri' 
				and dateadd(day,3,(date_part(year, V_DATE_1)||'-04-01')) = V_DATE_1 then
				'Holiday' 
				when monthname(V_DATE_1) ='Apr' and dayname(date_part(year, V_DATE_1)||'-04-01') = 'Sat' 
				and dateadd(day,2,(date_part(year, V_DATE_1)||'-04-01')) = V_DATE_1  then
				'Holiday' 
				when monthname(V_DATE_1) ='Apr' and dayname(date_part(year, V_DATE_1)||'-04-01') = 'Sun' 
				and dateadd(day,1,(date_part(year, V_DATE_1)||'-04-01')) = V_DATE_1 then
				'Holiday' 
				when monthname(V_DATE_1) ='Apr' and dayname(date_part(year, V_DATE_1)||'-04-01') = 'Mon' 
                and date_part(year, V_DATE_1)||'-04-01'= V_DATE_1 then
				'Holiday' 
				when monthname(V_DATE_1) ='Apr' and dayname(date_part(year, V_DATE_1)||'-04-01') = 'Tue' 
				and dateadd(day,6 ,(date_part(year, V_DATE_1)||'-04-01')) = V_DATE_1  then
				'Holiday'   
				when monthname(V_DATE_1) ='Sep' and dayname(date_part(year, V_DATE_1)||'-09-01') = 'Wed' 
				and dateadd(day,5,(date_part(year, V_DATE_1)||'-09-01')) = V_DATE_1  then
				'Holiday' 
				when monthname(V_DATE_1) ='Sep' and dayname(date_part(year, V_DATE_1)||'-09-01') = 'Thu' 
				and dateadd(day,4,(date_part(year, V_DATE_1)||'-09-01')) = V_DATE_1  then
				'Holiday' 
				when monthname(V_DATE_1) ='Sep' and dayname(date_part(year, V_DATE_1)||'-09-01') = 'Fri' 
				and dateadd(day,3,(date_part(year, V_DATE_1)||'-09-01')) = V_DATE_1 then
				'Holiday' 
				when monthname(V_DATE_1) ='Sep' and dayname(date_part(year, V_DATE_1)||'-09-01') = 'Sat' 
				and dateadd(day,2,(date_part(year, V_DATE_1)||'-09-01')) = V_DATE_1  then
				'Holiday' 
				when monthname(V_DATE_1) ='Sep' and dayname(date_part(year, V_DATE_1)||'-09-01') = 'Sun' 
				and dateadd(day,1,(date_part(year, V_DATE_1)||'-09-01')) = V_DATE_1 then
				'Holiday' 
				when monthname(V_DATE_1) ='Sep' and dayname(date_part(year, V_DATE_1)||'-09-01') = 'Mon' 
                and date_part(year, V_DATE_1)||'-09-01' = V_DATE_1 then
				'Holiday' 
				when monthname(V_DATE_1) ='Sep' and dayname(date_part(year, V_DATE_1)||'-09-01') = 'Tue' 
				and dateadd(day,6 ,(date_part(year, V_DATE_1)||'-09-01')) = V_DATE_1  then
				'Holiday' 
				when monthname(V_DATE_1) ='Nov' and dayname(date_part(year, V_DATE_1)||'-11-01') = 'Wed' 
				and dateadd(day,23,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1 then
				'Holiday'
				when monthname(V_DATE_1) ='Nov' and dayname(date_part(year, V_DATE_1)||'-11-01') = 'Thu' 
				and dateadd(day,22,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1 then
				'Holiday'
				when monthname(V_DATE_1) ='Nov' and dayname(date_part(year, V_DATE_1)||'-11-01') = 'Fri' 
				and dateadd(day,21,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1  then
				 'Holiday'
				when monthname(V_DATE_1) ='Nov' and dayname(date_part(year, V_DATE_1)||'-11-01') = 'Sat' 
				and dateadd(day,27,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1 then
				'Holiday'
				when monthname(V_DATE_1) ='Nov' and dayname(date_part(year, V_DATE_1)||'-11-01') = 'Sun' 
				and dateadd(day,26,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1 then
				'Holiday'
				when monthname(V_DATE_1) ='Nov' and dayname(date_part(year, V_DATE_1)||'-11-01') = 'Mon' 
				and dateadd(day,25,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1 then
				'Holiday'
				when monthname(V_DATE_1) ='Nov' and dayname(date_part(year, V_DATE_1)||'-11-01') = 'Tue' 
				and dateadd(day,24,(date_part(year, V_DATE_1)||'-11-01')) = V_DATE_1  then
				 'Holiday'     
				else
				'Not-Holiday' end as COMPANY_HOLIDAY_IND,
			case                                           
				when last_day(V_DATE_1) = V_DATE_1 then 
				'Month-end'
				else 'Not-Month-end' end as MONTH_END_IND,
					
			case when date_part(mm,date_trunc('week',V_DATE_1)) < 10 and date_part(dd,date_trunc('week',V_DATE_1)) < 10 then
					  date_part(yyyy,date_trunc('week',V_DATE_1))||'0'||
					  date_part(mm,date_trunc('week',V_DATE_1))||'0'||
					  date_part(dd,date_trunc('week',V_DATE_1))  
				 when date_part(mm,date_trunc('week',V_DATE_1)) < 10 and date_part(dd,date_trunc('week',V_DATE_1)) > 9 then
						date_part(yyyy,date_trunc('week',V_DATE_1))||'0'||
						date_part(mm,date_trunc('week',V_DATE_1))||date_part(dd,date_trunc('week',V_DATE_1))    
				 when date_part(mm,date_trunc('week',V_DATE_1)) > 9 and date_part(dd,date_trunc('week',V_DATE_1)) < 10 then
						date_part(yyyy,date_trunc('week',V_DATE_1))||date_part(mm,date_trunc('week',V_DATE_1))||
						'0'||date_part(dd,date_trunc('week',V_DATE_1))    
				when date_part(mm,date_trunc('week',V_DATE_1)) > 9 and date_part(dd,date_trunc('week',V_DATE_1)) > 9 then
						date_part(yyyy,date_trunc('week',V_DATE_1))||
						date_part(mm,date_trunc('week',V_DATE_1))||
						date_part(dd,date_trunc('week',V_DATE_1)) end as WEEK_BEGIN_DATE_NKEY,
			date_trunc('week',V_DATE_1) as WEEK_BEGIN_DATE,

			case when  date_part(mm,last_day(V_DATE_1,'week')) < 10 and date_part(dd,last_day(V_DATE_1,'week')) < 10 then
					  date_part(yyyy,last_day(V_DATE_1,'week'))||'0'||
					  date_part(mm,last_day(V_DATE_1,'week'))||'0'||
					  date_part(dd,last_day(V_DATE_1,'week')) 
				 when  date_part(mm,last_day(V_DATE_1,'week')) < 10 and date_part(dd,last_day(V_DATE_1,'week')) > 9 then
					  date_part(yyyy,last_day(V_DATE_1,'week'))||'0'||
					  date_part(mm,last_day(V_DATE_1,'week'))||date_part(dd,last_day(V_DATE_1,'week'))   
				 when  date_part(mm,last_day(V_DATE_1,'week')) > 9 and date_part(dd,last_day(V_DATE_1,'week')) < 10  then
					  date_part(yyyy,last_day(V_DATE_1,'week'))||date_part(mm,last_day(V_DATE_1,'week'))||'0'||
					  date_part(dd,last_day(V_DATE_1,'week'))   
				 when  date_part(mm,last_day(V_DATE_1,'week')) > 9 and date_part(dd,last_day(V_DATE_1,'week')) > 9 then
					  date_part(yyyy,last_day(V_DATE_1,'week'))||
					  date_part(mm,last_day(V_DATE_1,'week'))||
					  date_part(dd,last_day(V_DATE_1,'week')) end as WEEK_END_DATE_NKEY,
			last_day(V_DATE_1,'week') as WEEK_END_DATE,
			week(V_DATE_1) as WEEK_NUM_IN_YEAR,
			case when monthname(V_DATE_1) ='Jan' then 'January'
				   when monthname(V_DATE_1) ='Feb' then 'February'
				   when monthname(V_DATE_1) ='Mar' then 'March'
				   when monthname(V_DATE_1) ='Apr' then 'April'
				   when monthname(V_DATE_1) ='May' then 'May'
				   when monthname(V_DATE_1) ='Jun' then 'June'
				   when monthname(V_DATE_1) ='Jul' then 'July'
				   when monthname(V_DATE_1) ='Aug' then 'August'
				   when monthname(V_DATE_1) ='Sep' then 'September'
				   when monthname(V_DATE_1) ='Oct' then 'October'
				   when monthname(V_DATE_1) ='Nov' then 'November'
				   when monthname(V_DATE_1) ='Dec' then 'December' end as MONTH_NAME,
			monthname(V_DATE_1) as MONTH_ABBREV,
			month(V_DATE_1) as MONTH_NUM_IN_YEAR,
			case when month(V_DATE_1) < 10 then 
			year(V_DATE_1)||'-0'||month(V_DATE_1)   
			else year(V_DATE_1)||'-'||month(V_DATE_1) end as YEARMONTH,
			quarter(V_DATE_1) as CURRENT_QUARTER,
			year(V_DATE_1)||'-0'||quarter(V_DATE_1) as YEARQUARTER,
			year(V_DATE_1) as CURRENT_YEAR,
			/*Modify the following based on company fiscal year - assumes Jan 01*/
            to_date(year(V_DATE_1)||'-01-01','YYYY-MM-DD') as FISCAL_CUR_YEAR,
            to_date(year(V_DATE_1) -1||'-01-01','YYYY-MM-DD') as FISCAL_PREV_YEAR,
			case when   V_DATE_1 < FISCAL_CUR_YEAR then
			datediff('week', FISCAL_PREV_YEAR,V_DATE_1)
			else 
			datediff('week', FISCAL_CUR_YEAR,V_DATE_1)  end as FISCAL_WEEK_NUM  ,
			decode(datediff('MONTH',FISCAL_CUR_YEAR, V_DATE_1)+1 ,-2,10,-1,11,0,12,
                   datediff('MONTH',FISCAL_CUR_YEAR, V_DATE_1)+1 ) as FISCAL_MONTH_NUM,
			concat( year(FISCAL_CUR_YEAR) 
				   ,case when to_number(FISCAL_MONTH_NUM) = 10 or 
							to_number(FISCAL_MONTH_NUM) = 11 or 
                            to_number(FISCAL_MONTH_NUM) = 12  then
							'-'||FISCAL_MONTH_NUM
					else  concat('-0',FISCAL_MONTH_NUM) end ) as FISCAL_YEARMONTH,
			case when quarter(V_DATE_1) = 4 then 4
				 when quarter(V_DATE_1) = 3 then 3
				 when quarter(V_DATE_1) = 2 then 2
				 when quarter(V_DATE_1) = 1 then 1 end as FISCAL_QUARTER,
			
			case when   V_DATE_1 < FISCAL_CUR_YEAR then
					year(FISCAL_CUR_YEAR)
					else year(FISCAL_CUR_YEAR)+1 end
					||'-0'||case when quarter(V_DATE_1) = 4 then 4
					 when quarter(V_DATE_1) = 3 then 3
					 when quarter(V_DATE_1) = 2 then 2
					 when quarter(V_DATE_1) = 1 then 1 end as FISCAL_YEARQUARTER,
			case when quarter(V_DATE_1) = 4  then 2 when quarter(V_DATE_1) = 3 then 2
				when quarter(V_DATE_1) = 1  then 1 when quarter(V_DATE_1) = 2 then 1
			end as FISCAL_HALFYEAR,
			year(FISCAL_CUR_YEAR) as FISCAL_YEAR,
			to_timestamp_ntz(V_DATE) as SQL_TIMESTAMP,
			'Y' as CURRENT_ROW_IND,
			to_date(current_timestamp) as EFFECTIVE_DATE,
			to_date('9999-12-31') as EXPIRA_DATE
			--from table(generator(rowcount => 8401)) /*<< Set to generate 20 years. Modify rowcount to increase or decrease size*/
	        from table(generator(rowcount => 730)) /*<< Set to generate 20 years. Modify rowcount to increase or decrease size*/
    )v;

--Miscellaneous queries
SELECT * FROM  DIM_DATE
--ORDER BY DATE;


--delete from DIM_DATE;

--DROP TABLE DIM_DATE;





--Second, create rest of the dimension tables

--DIM_LOCATION
--DROP TABLE DIM_LOCATION
CREATE OR REPLACE TABLE DIM_LOCATION (
    DimLocationID INT IDENTITY(1,1) CONSTRAINT PK_DimLocationID PRIMARY KEY NOT NULL,
    Address VARCHAR(255) NOT NULL,
    City VARCHAR(255) NOT NULL,
    PostalCode VARCHAR(255) NOT NULL,
    State_Province VARCHAR(255) NOT NULL,
    Country VARCHAR(255) NOT NULL
);

--Load unknown members
INSERT INTO DIM_LOCATION (
    DimLocationID,
    Address,
    City,
    PostalCode,
    State_Province,
    Country
)
VALUES (
    -1,
    'Unknown',
    'Unknown',
    'Unknown',
    'Unknown',
    'Unknown'
);
--Load data from STAGING_STORE, STAGING_RESELLER, STAGING_CUSTOMER.
INSERT INTO DIM_LOCATION (
    Address,
    City,
    PostalCode,
    State_Province,
    Country
)
SELECT
    Address,
    City,
    PostalCode,
    StateProvince,
    Country
FROM STAGING_STORE
UNION
SELECT
    Address,
    City,
    PostalCode,
    StateProvince,
    Country
FROM STAGING_RESELLER
UNION
SELECT
    Address,
    City,
    PostalCode,
    StateProvince,
    Country
FROM STAGING_CUSTOMER;

SELECT * FROM DIM_LOCATION

--DIM_STORE
--DROP TABLE DIM_STORE
CREATE OR REPLACE TABLE DIM_STORE (
    DimStoreID INT IDENTITY(1,1) CONSTRAINT PK_DimStoreID PRIMARY KEY NOT NULL,
    DimLocationID INT NOT NULL,
    SourceStoreID INT NOT NULL,
    StoreName VARCHAR(255) NOT NULL,
    StoreNumber INT NOT NULL,
    StoreManager VARCHAR(255) NOT NULL,
    CONSTRAINT FK_Store_Location FOREIGN KEY (DimLocationID) REFERENCES DIM_LOCATION(DimLocationID)
);

INSERT INTO DIM_STORE (
    DimStoreID,
    DimLocationID,
    SourceStoreID,
    StoreName,
    StoreNumber,
    StoreManager
)
VALUES (
    -1,
    -1,
    -1,
    'Unknown',
    -1,
    'Unknown'
);

INSERT INTO DIM_STORE (
    DimLocationID,
    SourceStoreID,
    StoreName,
    StoreNumber,
    StoreManager
)
SELECT
    COALESCE(L.DimLocationID, -1) AS DimLocationID,
    S.StoreID AS SourceStoreID,
    'Unknown' AS StoreName,
    S.StoreNumber,
    S.StoreManager
FROM STAGING_STORE S
LEFT JOIN DIM_LOCATION L
  ON S.Address = L.Address
 AND S.City = L.City
 AND S.PostalCode = L.PostalCode
 AND S.StateProvince = L.State_Province
 AND S.Country = L.Country;

SELECT * FROM DIM_STORE

--Source data do not contain the store name, run below code once it is updated. 
/**
UPDATE DIM_STORE
SET StoreName = s.StoreName
FROM DIM_STORE ds
JOIN STAGING_STORE s ON ds.SourceStoreID = s.StoreID
WHERE ds.StoreName = 'Unknown'
**/

--DIM_RESELLER
--DROP TABLE DIM_RESELLER
--TRUNCATE TABLE DIM_RESELLER
CREATE OR REPLACE TABLE DIM_RESELLER (
    DimResellerID INT IDENTITY(1,1) CONSTRAINT PK_DimResellerID PRIMARY KEY NOT NULL,
    DimLocationID INT NOT NULL,
    ResellerID VARCHAR(255) NOT NULL,
    ResellerName VARCHAR(255) NOT NULL,
    ContactName VARCHAR(255) NOT NULL,
    PhoneNumber VARCHAR(255) NOT NULL,
    Email VARCHAR(255) NOT NULL,
    CONSTRAINT FK_Reseller_Location FOREIGN KEY (DimLocationID) REFERENCES DIM_LOCATION(DimLocationID)
);

INSERT INTO DIM_RESELLER (
    DimResellerID,
    DimLocationID,
    ResellerID,
    ResellerName,
    ContactName,
    PhoneNumber,
    Email
)
VALUES (
    -1,
    -1,
    'Unknown',
    'Unknown',
    'Unknown',
    'Unknown',
    'Unknown'
);

INSERT INTO DIM_RESELLER (
    DimLocationID,
    ResellerID,
    ResellerName,
    ContactName,
    PhoneNumber,
    Email
)
SELECT
    COALESCE(L.DimLocationID, -1) AS DimLocationID,
    R.ResellerID,
    R.ResellerName,
    R.Contact AS ContactName,
    R.PhoneNumber,
    R.EmailAddress AS Email
FROM STAGING_RESELLER R
LEFT JOIN DIM_LOCATION L
  ON R.Address = L.Address
 AND R.City = L.City
 AND R.PostalCode = L.PostalCode
 AND R.StateProvince = L.State_Province
 AND R.Country = L.Country;

SELECT * FROM DIM_RESELLER

--DIM_CUSTOMER
--DROP TABLE DIM_CUSTOMER
CREATE OR REPLACE TABLE DIM_CUSTOMER (
    DimCustomerID INT IDENTITY(1,1) CONSTRAINT PK_DimCustomerID PRIMARY KEY NOT NULL,
    DimLocationID INT NOT NULL,
    CustomerID VARCHAR(255) NOT NULL,
    CustomerFullName VARCHAR(255) NOT NULL,
    CustomerFirstName VARCHAR(255) NOT NULL,
    CustomerLastName VARCHAR(255) NOT NULL,
    CustomerGender VARCHAR(255) NOT NULL,
    CONSTRAINT FK_Customer_Location FOREIGN KEY (DimLocationID) REFERENCES DIM_LOCATION(DimLocationID)
);

INSERT INTO DIM_CUSTOMER (
    DimCustomerID,
    DimLocationID,
    CustomerID,
    CustomerFullName,
    CustomerFirstName,
    CustomerLastName,
    CustomerGender
)
VALUES (
    -1,
    -1,
    'Unknown',
    'Unknown',
    'Unknown',
    'Unknown',
    'Unknown'
);

INSERT INTO DIM_CUSTOMER (
    DimLocationID,
    CustomerID,
    CustomerFullName,
    CustomerFirstName,
    CustomerLastName,
    CustomerGender
)
SELECT
    COALESCE(L.DimLocationID, -1) AS DimLocationID,
    C.CustomerID,
    C.FirstName|| ' ' ||C.LastName AS CustomerFullName,
    C.FirstName AS CustomerFirstName,
    C.LastName AS CustomerLastName,
    C.Gender AS CustomerGender
FROM STAGING_CUSTOMER C
LEFT JOIN DIM_LOCATION L
  ON C.Address = L.Address
 AND C.City = L.City
 AND C.PostalCode = L.PostalCode
 AND C.StateProvince = L.State_Province
 AND C.Country = L.Country;

 SELECT * FROM DIM_CUSTOMER

--DIM_CHANNEL
--DROP TABLE DIM_CHANNEL
CREATE OR REPLACE TABLE DIM_CHANNEL (
    DimChannelID INT IDENTITY(1,1) CONSTRAINT PK_DimChannelID PRIMARY KEY NOT NULL,
    ChannelID INT NOT NULL,
    ChannelCategoryID INT NOT NULL,
    ChannelName VARCHAR(255) NOT NULL,
    ChannelCategory VARCHAR(255) NOT NULL
);
INSERT INTO DIM_CHANNEL(
    DimChannelID,
    ChannelID,
    ChannelCategoryID,
    ChannelName,
    ChannelCategory
)
VALUES (
    -1,
    -1,
    -1,
    'Unknown',
    'Unknown'
); 

INSERT INTO DIM_CHANNEL(
    ChannelID,
    ChannelCategoryID,
    ChannelName,
    ChannelCategory
)
SELECT 
    CH.ChannelID,
    CH.ChannelCategoryID,
    CH.Channel AS ChannelName,
    CHC.ChannelCategory
FROM STAGING_CHANNEL CH
JOIN STAGING_CHANNELCATEGORY CHC
ON CH.ChannelCategoryID = CHC.ChannelCategoryID;

SELECT*FROM DIM_CHANNEL

--DIM_PRODUCT
--DROP TABLE DIM_PRODUCT

CREATE OR REPLACE TABLE DIM_PRODUCT (
    DimProductID INT IDENTITY(1,1) CONSTRAINT PK_DimProductID PRIMARY KEY NOT NULL,
    ProductID INT NOT NULL,
    ProductTypeID INT NOT NULL,
    ProductCategoryID INT NOT NULL,
    ProductName VARCHAR(255) NOT NULL,
    ProductType VARCHAR(255) NOT NULL,
    ProductCategory VARCHAR(255) NOT NULL,
    ProductRetailPrice FLOAT NOT NULL,
    ProductWholesalePrice FLOAT NOT NULL,
    ProductCost FLOAT NOT NULL,
    ProductRetailProfit FLOAT NOT NULL,
    ProductWholesaleUnitProfit FLOAT NOT NULL,
    ProductProfitMarginUnitPercent FLOAT NOT NULL
);
INSERT INTO DIM_PRODUCT (
    DimProductID,
    ProductID,
    ProductTypeID,
    ProductCategoryID,
    ProductName,
    ProductType,
    ProductCategory,
    ProductRetailPrice,
    ProductWholesalePrice,
    ProductCost,
    ProductRetailProfit,
    ProductWholesaleUnitProfit,
    ProductProfitMarginUnitPercent
)
VALUES (
    -1,
    -1,
    -1,
    -1,
    'Unknown',
    'Unknown',
    'Unknown',
    0,
    0,
    0,
    0,
    0,
    0
);

INSERT INTO DIM_PRODUCT (
    ProductID,
    ProductTypeID,
    ProductCategoryID,
    ProductName,
    ProductType,
    ProductCategory,
    ProductRetailPrice,
    ProductWholesalePrice,
    ProductCost,
    ProductRetailProfit,
    ProductWholesaleUnitProfit,
    ProductProfitMarginUnitPercent
)
SELECT
    P.ProductID,
    P.ProductTypeID,
    PC.ProductCategoryID,
    P.Product AS ProductName,
    PT.ProductType,
    PC.ProductCategory,
    P.Price AS ProductRetailPrice,
    P.WholesalePrice AS ProductWholesalePrice,
    P.Cost AS ProductCost,
    P.Price - P.Cost AS RetailProfit,
    P.WholesalePrice - P.Cost AS WholesaleUnitProfit,
    CASE
        WHEN P.Price = 0 THEN 0
        ELSE (P.Price - P.Cost) / P.Price
    END AS ProfitMarginUnitPercent
FROM STAGING_PRODUCT P
JOIN STAGING_PRODUCTTYPE PT ON P.ProductTypeID = PT.ProductTypeID
JOIN STAGING_PRODUCTCATEGORY PC ON PT.ProductCategoryID = PC.ProductCategoryID;

SELECT * FROM DIM_PRODUCT
```
## Third Step: Create and Load Fact Tables in Snowflake
In this step, fact tables were created to store measurable business events that support quantitative analysis in the data warehouse. These tables capture transactional data such as sales amounts, quantities, and dates, which can be aggregated and analyzed across various dimensions. Each fact table was built using SQL, with foreign keys linking to the related dimension tables, a clearly defined grain, and all null foreign keys mapped to unknown members to ensure data integrity.

```
--Create Fact Tables
DROP TABLE FACT_SRCSalesTarget
CREATE OR REPLACE TABLE FACT_SRCSalesTarget ( 
    DimStoreID INT CONSTRAINT FK_StoreID FOREIGN KEY REFERENCES DIM_STORE(DimStoreID),
    DimResellerID INT CONSTRAINT FK_ResellerID FOREIGN KEY REFERENCES DIM_RESELLER(DimResellerID),
    DimChannelID INT CONSTRAINT FK_ChannelID FOREIGN KEY REFERENCES DIM_CHANNEL(DimChannelID),
    DimTargetDateID NUMBER(9) CONSTRAINT FK_TargetDateID FOREIGN KEY REFERENCES DIM_DATE(DATE_PKEY),
    SalesTargetAmount FLOAT    
);

CREATE OR REPLACE TABLE FACT_ProductSalesTarget (
    DimProductID INT CONSTRAINT FK_ProductID FOREIGN KEY REFERENCES DIM_PRODUCT(DimProductID),
    DimTargetDateID NUMBER(9) CONSTRAINT FK_DateID FOREIGN KEY REFERENCES DIM_DATE(DATE_PKEY),
    ProductTargetSalesQuantity INT
);

CREATE OR REPLACE TABLE FACT_SalesActual (
    DimProductID INT CONSTRAINT FK_ProductID FOREIGN KEY REFERENCES DIM_PRODUCT(DimProductID),
    DimStoreID INT CONSTRAINT FK_StoreID FOREIGN KEY REFERENCES DIM_STORE(DimStoreID),
    DimResellerID INT CONSTRAINT FK_ResellerID FOREIGN KEY REFERENCES DIM_RESELLER(DimResellerID),
    DimCustomerID INT CONSTRAINT FK_CustomerID FOREIGN KEY REFERENCES DIM_CUSTOMER(DimCustomerID),
    DimChannelID INT CONSTRAINT FK_ChannelID FOREIGN KEY REFERENCES DIM_CHANNEL(DimChannelID),
    DimSaleDateID NUMBER(9) CONSTRAINT FK_SaleDateID FOREIGN KEY REFERENCES DIM_DATE(DATE_PKEY),
    DimLocationID INT CONSTRAINT FK_LocationID FOREIGN KEY REFERENCES DIM_LOCATION(DimLocationID),
    SalesHeaderID INT,
    SalesDetailID INT,
    SaleAmount FLOAT,
    SaleQuantity INT,
    SaleUnitPrice FLOAT,
    SaleExtendedCost FLOAT,
    SaleTotalProfit FLOAT
);

--Before inserting data into the fact table, we need to update data, ensuring data across tables are aligned in order to complete the task smoothly.

--Update StoreNmae from DIM_STORE
UPDATE DIM_STORE
SET StoreName = 'Store Number ' || StoreNumber
WHERE StoreName = 'Unknown'
  AND StoreNumber != -1;

--Update Channel Name from DIM_CHANNEL
UPDATE DIM_CHANNEL
SET ChannelName = 'Online'
WHERE ChannelName = 'On-line';

--Insert Data to FACT_SRCSalesTarget
TRUNCATE TABLE FACT_SRCSalesTarget;

INSERT INTO FACT_SRCSalesTarget (
    DimStoreID,
    DimResellerID,
    DimChannelID,
    DimTargetDateID,
    SalesTargetAmount
)
SELECT DISTINCT
    COALESCE(S.DimStoreID, -1),
    COALESCE(R.DimResellerID, -1),
    C.DimChannelID,
    D.DATE_PKEY AS DimTargetDateID,
    T.TargetSalesAmount AS SalesTargetAmount
FROM STAGING_TARGET_CHANNEL_RESELLER_STORE T
LEFT JOIN DIM_STORE S ON T.TargetName = S.StoreName
LEFT JOIN DIM_RESELLER R ON T.TargetName = R.ResellerName
JOIN DIM_CHANNEL C ON T.ChannelName = C.ChannelName
JOIN DIM_DATE D ON D.YEAR = T.Year;

--Check 
SELECT COUNT(*) FROM FACT_SRCSalesTarget;
--Check for Null Values
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN DimStoreID IS NULL THEN 1 ELSE 0 END) AS null_dimstoreid,
    SUM(CASE WHEN DimResellerID IS NULL THEN 1 ELSE 0 END) AS null_dimresellerid,
    SUM(CASE WHEN DimChannelID IS NULL THEN 1 ELSE 0 END) AS null_dimchannelid,
    SUM(CASE WHEN DimTargetDateID IS NULL THEN 1 ELSE 0 END) AS null_dimtargetid,
    SUM(CASE WHEN SalesTargetAmount IS NULL THEN 1 ELSE 0 END) AS null_salestargtamount
FROM FACT_SRCSALESTARGET



--Insert data to FACT_ProductSalesTarget
--TRUNCATE TABLE FACT_ProductSalesTarget;
INSERT INTO FACT_ProductSalesTarget (
    DimProductID,
    DimTargetDateID,
    ProductTargetSalesQuantity
)
SELECT DISTINCT
    P.DimProductID,
    D.DATE_PKEY AS DimTargetDateID,
    T.SalesQuantityTarget AS ProductTargetSalesQuantity
FROM STAGING_TARGET_PRODUCT T
JOIN DIM_PRODUCT P ON T.ProductID = P.ProductID
JOIN DIM_DATE D ON D.YEAR = T.Year
WHERE T.SalesQuantityTarget IS NOT NULL;

--Check
SELECT * FROM FACT_ProductSalesTarget

--Check for null values
SELECT
  COUNT(*) AS total_rows,
  SUM(CASE WHEN DimProductID IS NULL THEN 1 ELSE 0 END) AS null_product_id,
  SUM(CASE WHEN DimTargetDateID IS NULL THEN 1 ELSE 0 END) AS null_target_date_id,
  SUM(CASE WHEN ProductTargetSalesQuantity IS NULL THEN 1 ELSE 0 END) AS null_sales_quantity
FROM FACT_ProductSalesTarget;

--Insert Data to FACT_SalesActual
--First, change the date column of the STAGING_SALESHEADER table in order to join correctly with DIM_DATE. For example, year changes from 0013 to 2013

UPDATE STAGING_SALESHEADER
SET Date = DATEADD(YEAR, 2000, Date)
WHERE YEAR(Date) IN (13, 14);

--Sanity check
SELECT DISTINCT YEAR(Date), COUNT(*)
FROM STAGING_SALESHEADER
GROUP BY YEAR(Date)
ORDER BY YEAR(Date);

--Finally, insert data
TRUNCATE TABLE FACT_SalesActual;

INSERT INTO FACT_SalesActual (
    DimProductID,
    DimStoreID,
    DimResellerID,
    DimCustomerID,
    DimChannelID,
    DimSaleDateID,
    DimLocationID,
    SalesHeaderID,
    SalesDetailID,
    SaleAmount,
    SaleQuantity,
    SaleUnitPrice,
    SaleExtendedCost,
    SaleTotalProfit
)
SELECT DISTINCT
    P.DimProductID,
    COALESCE(S.DimStoreID, -1),
    COALESCE(R.DimResellerID, -1),
    COALESCE(CU.DimCustomerID, -1),
    C.DimChannelID,
    D.DATE_PKEY AS DimSaleDateID,
    CASE 
        WHEN S.DimLocationID IS NOT NULL AND S.DimLocationID != -1 THEN S.DimLocationID
        WHEN CU.DimLocationID IS NOT NULL AND CU.DimLocationID != -1 THEN CU.DimLocationID
        WHEN R.DimLocationID IS NOT NULL AND R.DimLocationID != -1 THEN R.DimLocationID
        ELSE -1
    END AS DimLocationID,
    SH.SalesHeaderID,
    SD.SalesDetailID,
    SD.SalesAmount,
    SD.SalesQuantity,
    CASE 
        WHEN COALESCE(R.DimResellerID, -1) = -1 THEN P.ProductRetailPrice
        ELSE P.ProductWholesalePrice 
    END AS SaleUnitPrice,
    SD.SalesQuantity * P.ProductCost AS SaleExtendedCost,
    CASE 
        WHEN COALESCE(R.DimResellerID, -1) = -1 THEN P.ProductRetailProfit
        ELSE P.ProductWholesaleUnitProfit 
    END * SD.SalesQuantity AS SaleTotalProfit
FROM STAGING_SALESDETAIL SD
JOIN STAGING_SALESHEADER SH ON SH.SalesHeaderID = SD.SalesHeaderID
LEFT JOIN DIM_PRODUCT P ON SD.ProductID = P.ProductID
LEFT JOIN DIM_STORE S ON COALESCE(SH.StoreID, -1) = S.SourceStoreID
LEFT JOIN DIM_RESELLER R ON COALESCE(SH.ResellerID, 'Unknown') = R.ResellerID
LEFT JOIN DIM_CUSTOMER CU ON COALESCE(SH.CustomerID, 'Unknown') = CU.CustomerID
LEFT JOIN DIM_CHANNEL C ON SH.ChannelID = C.ChannelID
LEFT JOIN DIM_DATE D ON SH.Date = D.DATE;

--Check for null values
SELECT
  COUNT(*) AS total_rows,
  SUM(CASE WHEN DimProductID IS NULL THEN 1 ELSE 0 END) AS null_product_id,
  SUM(CASE WHEN DimStoreID IS NULL THEN 1 ELSE 0 END) AS null_store_id,
  SUM(CASE WHEN DimResellerID IS NULL THEN 1 ELSE 0 END) AS null_reseller_id,
  SUM(CASE WHEN DimCustomerID IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
  SUM(CASE WHEN DimChannelID IS NULL THEN 1 ELSE 0 END) AS null_channel_id,
  SUM(CASE WHEN DimSaleDateID IS NULL THEN 1 ELSE 0 END) AS null_date_id,
  SUM(CASE WHEN DimLocationID IS NULL THEN 1 ELSE 0 END) AS null_location_id,
  SUM(CASE WHEN SalesHeaderID IS NULL THEN 1 ELSE 0 END) AS null_salesheader_id,
  SUM(CASE WHEN SalesDetailID IS NULL THEN 1 ELSE 0 END) AS null_salesdetail_id,
  SUM(CASE WHEN SaleAmount IS NULL THEN 1 ELSE 0 END) AS null_saleamount,
  SUM(CASE WHEN SaleQuantity IS NULL THEN 1 ELSE 0 END) AS null_salequantity,
  SUM(CASE WHEN SaleUnitPrice IS NULL THEN 1 ELSE 0 END) AS null_unitprice,
  SUM(CASE WHEN SaleExtendedCost IS NULL THEN 1 ELSE 0 END) AS null_extendedcost,
  SUM(CASE WHEN SaleTotalProfit IS NULL THEN 1 ELSE 0 END) AS null_totalprofit
FROM FACT_SalesActual;

--Test
SELECT * FROM FACT_SALESACTUAL 
```
## Fourth Step: Secure Views and Custom Queries for Visualizations
Secure SQL pass-through views were created to replicate each dimension and fact table, establishing a stable foundation for the data access layer. In addition, three custom views were designed to handle advanced logic such as filtering, grouping, and aggregation to support Tableau visualizations.

```
--Create Pass-Through View for all dimension tables and fact tables

--DIM_DATE
CREATE SECURE VIEW V_DIM_DATE AS
SELECT
    DATE_PKEY,
    DATE,
    FULL_DATE_DESC,
    DAY_NUM_IN_WEEK,
    DAY_NUM_IN_MONTH,
    DAY_NUM_IN_YEAR,
    DAY_NAME,
    DAY_ABBREV,
    WEEKDAY_IND,
    US_HOLIDAY_IND,
    _HOLIDAY_IND,
    MONTH_END_IND,
    WEEK_BEGIN_DATE_NKEY,
    WEEK_BEGIN_DATE,
    WEEK_END_DATE_NKEY,
    WEEK_END_DATE,
    WEEK_NUM_IN_YEAR,
    MONTH_NAME,
    MONTH_ABBREV,
    MONTH_NUM_IN_YEAR,
    YEARMONTH,
    QUARTER,
    YEARQUARTER,
    YEAR,
    FISCAL_WEEK_NUM,
    FISCAL_MONTH_NUM,
    FISCAL_YEARMONTH,
    FISCAL_QUARTER,
    FISCAL_YEARQUARTER,
    FISCAL_HALFYEAR,
    FISCAL_YEAR,
    SQL_TIMESTAMP,
    CURRENT_ROW_IND,
    EFFECTIVE_DATE,
    EXPIRATION_DATE
FROM DIM_DATE;

--DIM_LOCATION
CREATE SECURE VIEW V_DIM_LOCATION AS
SELECT
    DimLocationID,
    Address,
    City,
    PostalCode,
    State_Province,
    Country
FROM DIM_LOCATION;

--DIM_STORE
CREATE SECURE VIEW V_DIM_STORE AS
SELECT
    DimStoreID,
    DimLocationID,
    SourceStoreID,
    StoreName,
    StoreNumber,
    StoreManager
FROM DIM_STORE;

--DIM_RESELLER
CREATE SECURE VIEW V_DIM_RESELLER AS
SELECT
    DimResellerID,
    DimLocationID,
    ResellerID,
    ResellerName,
    ContactName,
    PhoneNumber,
    Email
FROM DIM_RESELLER;

--DIM_CUSTOMER
CREATE SECURE VIEW V_DIM_CUSTOMER AS
SELECT
    DimCustomerID,
    DimLocationID,
    CustomerID,
    CustomerFullName,
    CustomerFirstName,
    CustomerLastName,
    CustomerGender
FROM DIM_CUSTOMER;

--DIM_CHANNEL
CREATE SECURE VIEW V_DIM_CHANNEL AS
SELECT
    DimChannelID,
    ChannelID,
    ChannelCategoryID,
    ChannelName,
    ChannelCategory
FROM DIM_CHANNEL;

--DIM_PRODUCT
CREATE SECURE VIEW V_DIM_PRODUCT AS
SELECT
    DimProductID,
    ProductID,
    ProductTypeID,
    ProductCategoryID,
    ProductName,
    ProductType,
    ProductCategory,
    ProductRetailPrice,
    ProductWholesalePrice,
    ProductCost,
    ProductRetailProfit,
    ProductWholesaleUnitProfit,
    ProductProfitMarginUnitPercent
FROM DIM_PRODUCT;

--FACT_SRCSalesTarget
CREATE SECURE VIEW V_FACT_SRCSalesTarget AS
SELECT
    DimStoreID,
    DimResellerID,
    DimChannelID,
    DimTargetDateID,
    SalesTargetAmount
FROM FACT_SRCSalesTarget;

--FACT_ProductSalesTarget
CREATE SECURE VIEW V_FACT_ProductSalesTarget AS
SELECT
    DimProductID,
    DimTargetDateID,
    ProductTargetSalesQuantity
FROM FACT_ProductSalesTarget;

--FACT_SalesActual
CREATE SECURE VIEW V_FACT_SalesActual AS
SELECT
    DimProductID,
    DimStoreID,
    DimResellerID,
    DimCustomerID,
    DimChannelID,
    DimSaleDateID,
    DimLocationID,
    SalesHeaderID,
    SalesDetailID,
    SaleAmount,
    SaleQuantity,
    SaleUnitPrice,
    SaleExtendedCost,
    SaleTotalProfit
FROM FACT_SalesActual;

--Custom Views to answer the questions

--1-1 2014 Sales Amount & Forecast
--DROP VIEW IF EXISTS SALES_FORECAST;
CREATE OR REPLACE VIEW SALES_FORECAST AS
SELECT 
  D.YEAR, 
  S.STORENAME, 
  S.DIMSTOREID, 
  AVG(ST.SALESTARGETAMOUNT) AS TARGET_SALES, 
  SUM(SA.SALEAMOUNT) AS TOTAL_SALES, 
  JO.JAN_OCT_2013,
  AVG(ST.SALESTARGETAMOUNT) - SUM(SA.SALEAMOUNT) AS TARGET_GAP,
  ND.NOV_DEC_2013
FROM DIM_STORE S
JOIN FACT_SRCSALESTARGET ST ON S.DIMSTOREID = ST.DIMSTOREID
JOIN DIM_DATE D ON ST.DIMTARGETDATEID = D.DATE_PKEY
JOIN FACT_SALESACTUAL SA ON ST.DIMSTOREID = SA.DIMSTOREID AND ST.DIMTARGETDATEID = SA.DIMSALEDATEID
LEFT JOIN (
  SELECT SA.DIMSTOREID, SUM(SA.SALEAMOUNT) AS NOV_DEC_2013
  FROM FACT_SALESACTUAL SA
  JOIN DIM_DATE D ON SA.DIMSALEDATEID = D.DATE_PKEY
  WHERE D.YEAR = 2013 AND D.MONTH_NUM_IN_YEAR IN (11, 12)
  GROUP BY SA.DIMSTOREID
) ND ON S.DIMSTOREID = ND.DIMSTOREID
LEFT JOIN (
  SELECT SA.DIMSTOREID, SUM(SA.SALEAMOUNT) AS JAN_OCT_2013
  FROM FACT_SALESACTUAL SA
  JOIN DIM_DATE D ON SA.DIMSALEDATEID = D.DATE_PKEY
  WHERE D.YEAR = 2013 AND D.MONTH_NUM_IN_YEAR BETWEEN 1 AND 10
  GROUP BY SA.DIMSTOREID
) JO ON S.DIMSTOREID = JO.DIMSTOREID
WHERE S.DIMSTOREID IN (4, 6) AND D.YEAR = 2014
GROUP BY D.YEAR, S.STORENAME, S.DIMSTOREID, ND.NOV_DEC_2013, JO.JAN_OCT_2013
ORDER BY S.DIMSTOREID;

select * from sales_forecast
--1-2 Check how many months have passed in 2014
select d.year, d.month_num_in_year, sa.dimstoreid, SUM(sa.saleamount) as sum_amount
from fact_salesactual sa
join dim_date d on sa.dimsaledateid = d.date_pkey
where dimstoreid IN (6) and d.year = 2014
group by d.year, d.month_num_in_year, sa.dimstoreid
order by d.year, d.month_num_in_year;



--2 Men’s Casual and Women’s Casual Bonus Pool 
--DROP VIEW IF EXISTS V_MENS_WOMENS_CASUAL_BONUS;
CREATE OR REPLACE VIEW MENS_WOMENS_CASUAL_BONUS AS
SELECT 
    D.YEAR, 
    S.DIMSTOREID, 
    S.STORENAME, 
    SUM(SA.SALEAMOUNT) AS TTL_SALE,
    SUM(SUM(SA.SALEAMOUNT)) OVER (PARTITION BY D.YEAR) AS TTL_OVERALL,
    SUM(SA.SALEAMOUNT) * 1.0 / SUM(SUM(SA.SALEAMOUNT)) OVER (PARTITION BY D.YEAR) AS CONTRIBUTION_PCT,
    CASE 
      WHEN D.YEAR = 2013 THEN SUM(SA.SALEAMOUNT) * 1.0 / SUM(SUM(SA.SALEAMOUNT)) OVER (PARTITION BY D.YEAR) * 500000
      WHEN D.YEAR = 2014 THEN SUM(SA.SALEAMOUNT) * 1.0 / SUM(SUM(SA.SALEAMOUNT)) OVER (PARTITION BY D.YEAR) * 400000
    END AS BONUS_AMOUNT
FROM DIM_PRODUCT P
JOIN FACT_SALESACTUAL SA ON P.DIMPRODUCTID = SA.DIMPRODUCTID 
JOIN DIM_DATE D ON SA.DIMSALEDATEID = D.DATE_PKEY
JOIN DIM_STORE S ON SA.DIMSTOREID = S.DIMSTOREID
WHERE P.PRODUCTTYPE IN ('Men''s Casual', 'Women''s Casual') AND S.DIMSTOREID != -1
GROUP BY D.YEAR, S.DIMSTOREID, S.STORENAME;

select * from MENS_WOMENS_CASUAL_BONUS
--3.Assess product sales by day of the week at stores 5 and 8
CREATE OR REPLACE VIEW STORE_5_8_WEEK_SALES AS
SELECT 
  D.YEAR, 
  D.DAY_NUM_IN_WEEK,
  D.DAY_NAME, 
  S.STORENAME, 
  P.PRODUCTID, 
  P.PRODUCTNAME, 
  SUM(SA.SALEAMOUNT) AS TTL_SALE, 
  SUM(SA.SALEQUANTITY) AS TTL_QUANTITY
FROM DIM_PRODUCT P
JOIN FACT_SALESACTUAL SA ON P.DIMPRODUCTID = SA.DIMPRODUCTID
JOIN DIM_DATE D ON SA.DIMSALEDATEID = D.DATE_PKEY
JOIN DIM_STORE S ON SA.DIMSTOREID = S.DIMSTOREID
WHERE S.STORENAME IN ('Store Number 5', 'Store Number 8') 
GROUP BY 
  D.YEAR, 
  D.DAY_NUM_IN_WEEK,
  D.DAY_NAME, 
  S.STORENAME, 
  P.PRODUCTID, 
  P.PRODUCTNAME;



--4.Compare the performance of all stores located in states that have more than one store to all stores that are the only store in the state. What can we learn about having more than one store in a state?

CREATE OR REPLACE VIEW STATE_STORE_PERFORMANCE AS
SELECT 
  L.STATE_PROVINCE, 
  D.YEAR,
  COUNT(DISTINCT S.DIMSTOREID) AS COUNT_STORE_NUMBER, 
  SUM(SA.SALEAMOUNT) AS TTL_SALES,
  SUM(SA.SALEAMOUNT) * 1.0 / COUNT(DISTINCT S.DIMSTOREID) AS AVG_SALES_PER_STORE
FROM DIM_LOCATION L
JOIN DIM_STORE S ON S.DIMLOCATIONID = L.DIMLOCATIONID
JOIN FACT_SALESACTUAL SA ON S.DIMSTOREID = SA.DIMSTOREID
JOIN DIM_DATE D ON SA.DIMSALEDATEID = D.DATE_PKEY
WHERE L.STATE_PROVINCE <> 'Unknown'
GROUP BY L.STATE_PROVINCE, D.YEAR
ORDER BY L.STATE_PROVINCE, D.YEAR;

select * from state_store_performance
where year = 2013;
```

## Key Business Questions for Analysis
#### 1. Give an overall assessment of stores number 5 and 8’s sales.
- How are they performing compared to target? Will they meet their 2014 target?
- Should either store be closed? Why or why not?
- What should be done in the next year to maximize store profits?

#### 2. Recommend separate 2013 and 2014 bonus amounts for each store if the total bonus pool for 2013 is $500,000 and the total bonus pool for 2014 is $400,000. Base your recommendation on how well the stores are selling Product Types of Men’s Casual and Women’s Casual.

#### 3. Assess product sales by day of the week at stores 5 and 8. What can we learn about sales trends?

#### 4. Compare the performance of all stores located in states that have more than one store to all stores that are the only store in the state. What can we learn about having more than one store in a state?


## Visualization
Tableau was connected to the data warehouse using secure SQL views to power the visual analytics. A fully interactive dashboard was developed to answer key business questions, featuring multiple visualizations, global time filters, and user-driven interactions for deeper insights. [Click to view dashboard](https://public.tableau.com/app/profile/ching.ping.chan/viz/9_13AssignmentVisualization_17488860056310/Dashboard1)

<img width="1013" alt="Screen Shot 2025-06-27 at 7 26 46 PM" src="https://github.com/user-attachments/assets/89f9bac0-79c4-44a3-aecc-c442caa08e0f" />

