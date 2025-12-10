CREATE TABLE tkolios_chicago_weather AS
SELECT
  CAST(weather_date AS DATE) AS weather_day,
  prcp,
  snow,
  snwd,
  tmax,
  tmin
FROM tkolios_chicago_weather_raw;