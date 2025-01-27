## SQL Queries for the homework

1. During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, respectively, happened:

- Up to 1 mile
- In between 1 (exclusive) and 3 miles (inclusive),
- In between 3 (exclusive) and 7 miles (inclusive),
- In between 7 (exclusive) and 10 miles (inclusive),
- Over 10 miles

```
SELECT 
    COUNT(CASE WHEN trip_distance <= 1 THEN 1 END) as up_to_1_mile,
    COUNT(CASE WHEN trip_distance > 1 AND trip_distance <= 3 THEN 1 END) as between_1_and_3_miles,
    COUNT(CASE WHEN trip_distance > 3 AND trip_distance <= 7 THEN 1 END) as between_3_and_7_miles,
    COUNT(CASE WHEN trip_distance > 7 AND trip_distance <= 10 THEN 1 END) as between_7_and_10_miles,
    COUNT(CASE WHEN trip_distance > 10 THEN 1 END) as over_10_miles
FROM green_taxi_trips
WHERE lpep_pickup_datetime >= '2019-10-01' 
    AND lpep_pickup_datetime < '2019-11-01';
```

2. Which was the pick up day with the longest trip distance? Use the pick up time for your calculations.

Tip: For every day, we only care about one single trip with the longest distance.

```
SELECT 
    DATE(lpep_pickup_datetime) as pickup_date,
    MAX(trip_distance) as longest_trip
FROM green_taxi_trips
GROUP BY DATE(lpep_pickup_datetime)
ORDER BY pickup_date;
```

3. Which were the top pickup locations with over 13,000 in total_amount (across all trips) for 2019-10-18?

```
SELECT 
    z."Zone" as pickup_zone,
    SUM(t.total_amount) as total_amount
FROM green_taxi_trips t
JOIN zones z ON t."PULocationID" = z."LocationID"
WHERE DATE(t.lpep_pickup_datetime) = '2019-10-18'
GROUP BY z."Zone"
ORDER BY total_amount DESC;
```

4. For the passengers picked up in October 2019 in the zone named "East Harlem North" which was the drop off zone that had the largest tip?

```
SELECT 
    pickup."Zone" as pickup_zone,
    dropoff."Zone" as dropoff_zone,
    t.tip_amount
FROM green_taxi_trips t
JOIN zones pickup ON t."PULocationID" = pickup."LocationID"
JOIN zones dropoff ON t."DOLocationID" = dropoff."LocationID"
WHERE 
    DATE(t.lpep_pickup_datetime) >= '2019-10-01'
    AND DATE(t.lpep_pickup_datetime) < '2019-11-01'
    AND pickup.Zone = 'East Harlem North'
ORDER BY t.tip_amount DESC
LIMIT 1;
```



