CREATE EXTERNAL TABLE tkolios_chicago_crimes_raw (
  case_number     STRING,
  crime_date            STRING,
  block           STRING,
  primary_type    STRING,
  arrest          BOOLEAN,
  district        INT,
  ward            INT,
  community_area  INT,
  latitude        DOUBLE,
  longitude       DOUBLE
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tkolios/data/crimes'
TBLPROPERTIES (
  "skip.header.line.count" = "1"
);