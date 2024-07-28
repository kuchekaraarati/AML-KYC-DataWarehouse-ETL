-- Create the database 
CREATE DATABASE ETL_Process;

USE ETL_Process;

--Create table Raw_CustomerData
CREATE TABLE Raw_CustomerData(
    CustomerID INT PRIMARY KEY,
    Name VARCHAR(100),
    DateOfBirth DATE,
    Address VARCHAR(255),
    Country VARCHAR(50)
);

-- BULK INSERT Command in Raw_CustomerData
BULK INSERT Raw_CustomerData
FROM 'C:\Users\kuche\Desktop\Raw_CustomerData.csv'
WITH (
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    CODEPAGE = '65001', -- This specifies UTF-8 encoding
    TABLOCK
);

--Create table Raw_TransactionData
CREATE TABLE Raw_TransactionData (
    TransactionID INT PRIMARY KEY,
    CustomerID INT,
    TransactionDate DATE,
    Amount DECIMAL(10, 2),
    TransactionType VARCHAR(50)
);

--BULK INSERT Command in Raw_TransactionData
BULK INSERT Raw_TransactionData
FROM 'C:\Users\kuche\Desktop\Raw_TransactionData.csv'
WITH (
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    CODEPAGE = '65001', -- This specifies UTF-8 encoding
    TABLOCK
);
select * from Raw_CustomerData;
select * from Raw_TransactionData;

-- Staging Tables
CREATE TABLE Staging_CustomerData (
    CustomerID INT PRIMARY KEY,
    Name VARCHAR(100),
    DateOfBirth DATE,
    Address VARCHAR(255),
    Country VARCHAR(50)
);

CREATE TABLE Staging_TransactionData (
    TransactionID INT PRIMARY KEY,
    CustomerID INT,
    TransactionDate DATE,
    Amount DECIMAL(10, 2),
    TransactionType VARCHAR(50)
);

-- ETL Process
INSERT INTO Staging_CustomerData (CustomerID, Name, DateOfBirth, Address, Country)
SELECT CustomerID, Name, DateOfBirth, Address, Country
FROM Raw_CustomerData;

INSERT INTO Staging_TransactionData (TransactionID, CustomerID, TransactionDate, Amount, TransactionType)
SELECT TransactionID, CustomerID, TransactionDate, Amount, TransactionType
FROM Raw_TransactionData;

-- Data Warehouse Tables
CREATE TABLE DWH_Customers (
    CustomerID INT PRIMARY KEY,
    Name VARCHAR(100),
    DateOfBirth DATE,
    Address VARCHAR(255),
    Country VARCHAR(50),
    RiskLevel VARCHAR(50)
);

CREATE TABLE DWH_Transactions (
    TransactionID INT PRIMARY KEY,
    CustomerID INT,
    TransactionDate DATE,
    Amount DECIMAL(10, 2),
    TransactionType VARCHAR(50),
    SuspiciousFlag BIT
);

-- Insert Data into Data Warehouse Tables
INSERT INTO DWH_Customers (CustomerID, Name, DateOfBirth, Address, Country, RiskLevel)
SELECT 
    CustomerID,
    Name,
    DateOfBirth,
    Address,
    Country,
    CASE 
        WHEN Country IN ('High-Risk Country1', 'High-Risk Country2') THEN 'High'
        ELSE 'Low'
    END AS RiskLevel
FROM Staging_CustomerData;

INSERT INTO DWH_Transactions (TransactionID, CustomerID, TransactionDate, Amount, TransactionType, SuspiciousFlag)
SELECT 
    TransactionID,
    CustomerID,
    TransactionDate,
    Amount,
    TransactionType,
    CASE 
        WHEN Amount > 10000 THEN 1
        ELSE 0
    END AS SuspiciousFlag
FROM Staging_TransactionData;
