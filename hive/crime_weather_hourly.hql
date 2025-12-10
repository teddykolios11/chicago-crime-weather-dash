CREATE TABLE tkolios_crime_weather_hourly AS
SELECT
    TO_DATE(c.crime_ts) AS day,
    HOUR(c.crime_ts) AS hour,
    COUNT(*) AS crime_count,
    w.tmax, w.tmin, w.prcp, w.snow, w.snwd
FROM tkolios_chicago_crimes c
LEFT JOIN tkolios_chicago_weather w
    ON TO_DATE(c.crime_ts) = w.weather_day
GROUP BY
    TO_DATE(c.crime_ts),
    HOUR(c.crime_ts),
    w.tmax, w.tmin, w.prcp, w.snow, w.snwd;