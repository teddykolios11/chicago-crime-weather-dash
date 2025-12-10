CREATE TABLE tkolios_crimes_with_weather AS
SELECT
    c.*,
    w.prcp,
    w.snow,
    w.snwd,
    w.tmax,
    w.tmin
FROM tkolios_chicago_crimes c
LEFT JOIN tkolios_chicago_weather w
  ON TO_DATE(c.crime_timestamp) = w.weather_day;