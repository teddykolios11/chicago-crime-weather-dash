CREATE EXTERNAL TABLE tkolios_chicago_weather_raw (
  weather_date STRING,
  prcp         DOUBLE,
  snow         DOUBLE,
  snwd         DOUBLE,
  tmax         DOUBLE,
  tmin         DOUBLE
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tkolios/data/weather'
TBLPROPERTIES (
  "skip.header.line.count" = "1"
);