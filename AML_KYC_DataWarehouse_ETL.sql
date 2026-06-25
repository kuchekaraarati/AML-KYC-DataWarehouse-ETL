-- =====================================================
-- CREATE DATABASE
-- =====================================================

CREATE DATABASE AML_KYC_DWH;

USE AML_KYC_DWH;

-- =====================================================
-- RAW TABLES
-- =====================================================
SELECT TOP 10 * FROM Raw_Customers;
 
SELECT TOP 10 * FROM Raw_Accounts;

SELECT TOP 10 * FROM Raw_Transactions;

SELECT COUNT(*) FROM Raw_Customers;
SELECT COUNT(*) FROM Raw_Accounts;
SELECT COUNT(*) FROM Raw_Transactions;

-- =====================================================
-- STAGING TABLES
-- =====================================================

CREATE TABLE Stg_Customers
(
CustomerID INT PRIMARY KEY,
CustomerName VARCHAR(100),
DateOfBirth DATE,
Gender VARCHAR(20),
Occupation VARCHAR(100),
City VARCHAR(100),
Country VARCHAR(100),
AccountOpenDate DATE,
CustomerStatus VARCHAR(20)
);

INSERT INTO Stg_Customers
SELECT *
FROM Raw_Customers;

SELECT COUNT(*) FROM Stg_Customers;

CREATE TABLE Stg_Accounts
(
AccountID INT PRIMARY KEY,
CustomerID INT,
AccountType VARCHAR(50),
OpenDate DATE,
Balance DECIMAL(18,2)
);

INSERT INTO Stg_Accounts
SELECT *
FROM Raw_Accounts;

SELECT COUNT(*) FROM Stg_Accounts;

CREATE TABLE Stg_Transactions
(
TransactionID INT PRIMARY KEY,
AccountID INT,
TransactionDate DATE,
TransactionType VARCHAR(50),
Amount DECIMAL(18,2),
Channel VARCHAR(50),
Country VARCHAR(100)
);

INSERT INTO Stg_Transactions
SELECT *
FROM Raw_Transactions;

SELECT COUNT(*) FROM Stg_Transactions;

-- =====================================================
-- DIMENSION TABLES
-- =====================================================

CREATE TABLE DimCustomer
(
CustomerKey INT IDENTITY(1,1) PRIMARY KEY,
CustomerID INT,
CustomerName VARCHAR(100),
DateOfBirth DATE,
Gender VARCHAR(20),
Occupation VARCHAR(100),
City VARCHAR(100),
Country VARCHAR(100),
CustomerStatus VARCHAR(20),
RiskLevel VARCHAR(20)
);

INSERT INTO DimCustomer
(
    CustomerID,
    CustomerName,
    DateOfBirth,
    Gender,
    Occupation,
    City,
    Country,
    CustomerStatus,
    RiskLevel
)
SELECT
    CustomerID,
    CustomerName,
    DateOfBirth,
    Gender,
    Occupation,
    City,
    Country,
    CustomerStatus,
    CASE
        WHEN Country IN ('Iran','North Korea','Syria','Afghanistan')
        THEN 'High'
        ELSE 'Low'
    END
FROM Stg_Customers;

SELECT COUNT(*) AS DimCustomerCount
FROM DimCustomer;

CREATE TABLE DimAccount
(
AccountKey INT IDENTITY(1,1) PRIMARY KEY,
AccountID INT,
CustomerID INT,
AccountType VARCHAR(50),
OpenDate DATE,
Balance DECIMAL(18,2)
);

INSERT INTO DimAccount
(
    AccountID,
    CustomerID,
    AccountType,
    OpenDate,
    Balance
)
SELECT
    AccountID,
    CustomerID,
    AccountType,
    OpenDate,
    Balance
FROM Stg_Accounts;

SELECT COUNT(*) AS DimAccountCount
FROM DimAccount;

SELECT COUNT(*) FROM DimCustomer;
SELECT COUNT(*) FROM DimAccount;


CREATE TABLE DimDate
(
DateKey INT PRIMARY KEY,
FullDate DATE,
DayNumber INT,
MonthNumber INT,
MonthName VARCHAR(20),
QuarterNumber INT,
YearNumber INT
);

INSERT INTO DimDate
(
    DateKey,
    FullDate,
    DayNumber,
    MonthNumber,
    MonthName,
    QuarterNumber,
    YearNumber
)
SELECT DISTINCT
    CAST(CONVERT(VARCHAR(8), TransactionDate, 112) AS INT) AS DateKey,
    TransactionDate,
    DAY(TransactionDate),
    MONTH(TransactionDate),
    DATENAME(MONTH, TransactionDate),
    DATEPART(QUARTER, TransactionDate),
    YEAR(TransactionDate)
FROM Stg_Transactions;

SELECT COUNT(*) AS DimDateCount
FROM DimDate;

-- =====================================================
-- FACT TABLE
-- =====================================================

CREATE TABLE FactTransactions
(
TransactionKey INT IDENTITY(1,1) PRIMARY KEY,
TransactionID INT,
AccountKey INT,
CustomerKey INT,
DateKey INT,
TransactionType VARCHAR(50),
Amount DECIMAL(18,2),
Channel VARCHAR(50),
Country VARCHAR(100),
SuspiciousFlag BIT,

FOREIGN KEY (AccountKey)
    REFERENCES DimAccount(AccountKey),

FOREIGN KEY (CustomerKey)
    REFERENCES DimCustomer(CustomerKey),

FOREIGN KEY (DateKey)
    REFERENCES DimDate(DateKey)
);

INSERT INTO FactTransactions
(
    TransactionID,
    AccountKey,
    CustomerKey,
    DateKey,
    TransactionType,
    Amount,
    Channel,
    Country,
    SuspiciousFlag
)
SELECT
    t.TransactionID,
    da.AccountKey,
    dc.CustomerKey,
    CAST(CONVERT(VARCHAR(8), t.TransactionDate, 112) AS INT),
    t.TransactionType,
    t.Amount,
    t.Channel,
    t.Country,
    CASE
        WHEN t.Amount >= 50000 THEN 1
        ELSE 0
    END
FROM Stg_Transactions t
INNER JOIN DimAccount da
    ON t.AccountID = da.AccountID
INNER JOIN DimCustomer dc
    ON da.CustomerID = dc.CustomerID;

SELECT COUNT(*) AS FactTransactionCount
FROM FactTransactions;