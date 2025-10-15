CREATE EXTERNAL TABLE IF NOT EXISTS hack2_aw_catalog.ext_employees (
  EmployeeID         INT,
  ManagerID          INT,
  FirstName          STRING,
  LastName           STRING,
  FullName           STRING,
  JobTitle           STRING,
  OrganizationLevel  INT,
  MaritalStatus      STRING,
  Gender             STRING,
  Territory          STRING,
  Country            STRING,
  GroupCol           STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar'='\"', 'escapeChar'='\\')
LOCATION 's3://bg-hack2-aw-datalake2/bronze/source=github/table=employee/'
TBLPROPERTIES ('skip.header.line.count'='1');

SELECT * FROM ext_customers LIMIT 5;
