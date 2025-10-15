INSERT INTO hack2_aw_catalog.dim_employee
SELECT
  TRY(CAST(EmployeeID        AS INT)) AS EmployeeKey,
  TRY(CAST(ManagerID         AS INT)) AS ManagerID,
  TRIM(FirstName)                     AS FirstName,
  TRIM(LastName)                      AS LastName,
  TRIM(FullName)                      AS FullName,
  TRIM(JobTitle)                      AS JobTitle,
  TRY(CAST(OrganizationLevel AS INT)) AS OrganizationLevel,
  UPPER(TRIM(MaritalStatus))          AS MaritalStatus,
  UPPER(TRIM(Gender))                 AS Gender,
  TRIM(Territory)                     AS Territory,
  TRIM(Country)                       AS Country,
  TRIM(GroupCol)                      AS GroupCol
FROM hack2_aw_catalog.ext_employees
WHERE TRY(CAST(EmployeeID AS INT)) IS NOT NULL;
