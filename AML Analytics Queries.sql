--1. High-Risk Customer Distribution
SELECT
    RiskLevel,
    COUNT(*) AS CustomerCount
FROM DimCustomer
GROUP BY RiskLevel;

--2. Suspicious Transactions Count
SELECT
    COUNT(*) AS SuspiciousTransactionCount
FROM FactTransactions
WHERE SuspiciousFlag = 1;

--3. Total Transaction Amount
SELECT
    SUM(Amount) AS TotalTransactionAmount
FROM FactTransactions;

--4. Transactions by Type
SELECT
    TransactionType,
    COUNT(*) AS TransactionCount,
    SUM(Amount) AS TotalAmount
FROM FactTransactions
GROUP BY TransactionType;

--5. Monthly Transaction Trend
SELECT
    d.YearNumber,
    d.MonthName,
    SUM(f.Amount) AS TotalAmount
FROM FactTransactions f
JOIN DimDate d
ON f.DateKey = d.DateKey
GROUP BY
    d.YearNumber,
    d.MonthNumber,
    d.MonthName
ORDER BY
    d.YearNumber,
    d.MonthNumber;

--6. Top 10 Customers by Transaction Amount
SELECT TOP 10
    c.CustomerName,
    SUM(f.Amount) AS TotalAmount
FROM FactTransactions f
JOIN DimCustomer c
ON f.CustomerKey = c.CustomerKey
GROUP BY c.CustomerName
ORDER BY TotalAmount DESC;

--7. High-Risk Customers with Suspicious Transactions
SELECT
    c.CustomerName,
    c.Country,
    COUNT(*) AS SuspiciousTransactions,
    SUM(f.Amount) AS TotalAmount
FROM FactTransactions f
JOIN DimCustomer c
ON f.CustomerKey = c.CustomerKey
WHERE c.RiskLevel='High'
AND f.SuspiciousFlag=1
GROUP BY
    c.CustomerName,
    c.Country
ORDER BY TotalAmount DESC;