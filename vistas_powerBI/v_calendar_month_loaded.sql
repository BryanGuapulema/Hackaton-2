CREATE OR REPLACE VIEW hack2_aw_catalog.v_calendar_month_loaded AS
SELECT
  -- Convierte 'YYYY-MM' → primer día del mes como DATE
  CAST(date_parse(concat(f.RunMonth, '-01'), '%Y-%m-%d') AS DATE)         AS MonthDate,
  f.RunMonth                                                             AS YearMonth,
  CAST(substr(f.RunMonth,1,4) AS INTEGER)                                 AS Year,
  CAST(substr(f.RunMonth,6,2) AS INTEGER)                                 AS Month,
  date_format(date_parse(concat(f.RunMonth, '-01'), '%Y-%m-%d'), '%b')    AS MonthShort,
  quarter(date_parse(concat(f.RunMonth, '-01'), '%Y-%m-%d'))              AS Quarter
FROM hack2_aw_catalog.fact_sales f
GROUP BY 1,2,3,4,5,6
ORDER BY 1;
