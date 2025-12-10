CREATE TABLE tkolios_chicago_crimes AS
SELECT
  case_number,
  from_unixtime(unix_timestamp(crime_date, 'MM/dd/yyyy hh:mm:ss a')) AS crime_ts,
  block,
  primary_type,
  arrest,
  district,
  ward,
  community_area,
  latitude,
  longitude
FROM tkolios_chicago_crimes_raw;