/*
This script creates temp tables for customer segmentation based on activity within the following constraints:

Time Cohort:
Sessions after 2023-01-04 (after the new year holiday).

Behaviour Cohort:
Users that had more than 7 sessions during that period OR users that booked at least 2 trips during that period.
*/

-- QUERY 1: Temp table for sessions in our cohort.
CREATE TEMP TABLE filtered_sessions AS

-- Returns user_ids that had more than 7 sessions OR booked at least 2 trips after 4th Jan 2023.
WITH active_users AS (
    SELECT user_id
    FROM sessions
    WHERE DATE_TRUNC('day', session_start) > '2023-01-04'
    GROUP BY user_id
    HAVING COUNT(session_id) > 7
  		OR COUNT(DISTINCT trip_id) >= 2
),

	-- Keep only sessions after the cutoff date and only of active users via an inner join with the 1st CTE.
	active_users_sessions AS (
    SELECT sessions.*
    FROM sessions
    INNER JOIN active_users
      ON sessions.user_id = active_users.user_id
    WHERE DATE_TRUNC('day', session_start) > '2023-01-04'
)

/*
Join the flights and hotels tables to the 2nd CTE.
Apply a 1 night penalty for users that did not cancel their hotel on our app, this appears as negative nights on our system.
Discounts are applied to the hotel and flight price columns and corresponding savings columns are added.
*/
SELECT active_users_sessions.*,
	 ROUND(EXTRACT(EPOCH FROM session_end - session_start), 0) AS session_length_seconds,
	 hotel_name,
	 CASE WHEN nights < 1 THEN 1 ELSE nights END AS nights,
	 hotels.rooms,
	 hotels.check_in_time,
	 hotels.check_out_time,
	 hotels.hotel_per_room_usd * (1 - COALESCE(hotel_discount_amount, 0)) AS final_hotel_per_room_usd,
	 hotels.hotel_per_room_usd * COALESCE(hotel_discount_amount, 0) AS hotel_savings_per_room_usd,
	 flights.origin_airport,
	 flights.destination,
	 flights.destination_airport,
	 flights.seats, 
	 flights.return_flight_booked,
	 flights.departure_time,
	 flights.return_time,
	 flights.checked_bags,
	 flights.trip_airline,
	 flights.destination_airport_lat,
	 flights.destination_airport_lon,
	 flights.base_fare_usd * (1 - COALESCE(flight_discount_amount, 0)) AS final_fare_usd,
	 flights.base_fare_usd * COALESCE(flight_discount_amount, 0) AS fare_savings
FROM active_users_sessions
LEFT JOIN hotels
	ON active_users_sessions.trip_id = hotels.trip_id
LEFT JOIN flights
	ON active_users_sessions.trip_id = flights.trip_id
;

-- QUERY 2: Temp table for completed trips.
CREATE TEMP TABLE completed_trips AS

-- Return trip_id of cancelled trips.
WITH cancelled_trips AS (
  	SELECT trip_id AS cancel_id
		FROM filtered_sessions
		WHERE cancellation = TRUE
),

	-- Filter out trips that were cancelled.
	non_cancelled_trips AS (
    SELECT sessions.*
    FROM filtered_sessions AS sessions
    LEFT JOIN cancelled_trips AS cancelled
    	ON sessions.trip_id = cancelled.cancel_id
    WHERE sessions.trip_id IS NOT NULL
    	AND cancelled.cancel_id IS NULL
)

/*
Calculated columns for:
Time between booking session to start of trip.
Distance flown with Haversine formula, used to calculate the distance between two locations on a sphere (Earth).
Trip length depending on booking options.
Total cost of hotel, flights as well as the whole trip.
*/
SELECT trips.*,
  EXTRACT(DAY FROM
    CASE WHEN flight_booked = True
      THEN CASE WHEN hotel_booked = False THEN departure_time - session_end
      WHEN  departure_time < check_in_time THEN departure_time - session_end
      WHEN  departure_time > check_in_time THEN check_in_time - session_end
      END
    WHEN hotel_booked = True
    THEN check_in_time - session_end
  END) AS booking_to_trip_days,
  2 * 6371 * asin(sqrt(
    pow(sin((radians(destination_airport_lat) -
    radians(home_airport_lat)) / 2), 2) +
    cos(radians(home_airport_lat)) *
    cos(radians(destination_airport_lat)) *
    pow(sin((radians(destination_airport_lon) -
    radians(home_airport_lon)) / 2), 2)
  )) AS distance_flown_km,
  CASE WHEN flight_booked = True
    THEN CASE WHEN return_flight_booked = True
      THEN EXTRACT(DAY FROM return_time - departure_time) 
      WHEN hotel_booked = True
      THEN EXTRACT(DAY FROM check_out_time - departure_time)
      WHEN hotel_booked = False
      THEN 0 END
    WHEN hotel_booked = True
    THEN EXTRACT(DAY FROM check_out_time - check_in_time)
  END AS trip_length_days,
  final_hotel_per_room_usd * rooms * nights AS hotel_total_usd,
  final_fare_usd * seats AS flight_total_usd,
  COALESCE((final_hotel_per_room_usd * rooms * nights),0) +
  	COALESCE((final_fare_usd * seats),0) AS trip_total_usd
FROM non_cancelled_trips AS trips
JOIN users
	ON trips.user_id = users.user_id
;

-- QUERY 3 & 4: Add columns and apply further calculations using the distance_flown_km that was created in the second query.
ALTER TABLE completed_trips
ADD COLUMN flight_cost_per_km NUMERIC,
ADD COLUMN cost_saved_per_km NUMERIC
;

UPDATE completed_trips
SET flight_cost_per_km = CASE WHEN distance_flown_km > 0
    THEN flight_total_usd / distance_flown_km 
    ELSE NULL END,
	cost_saved_per_km = CASE WHEN distance_flown_km > 0
  	THEN fare_savings / distance_flown_km 
    ELSE NULL END
;


-- QUERY 5: Create an aggregated temp table of completed trips grouped by user.
CREATE TEMP TABLE user_agg_completed_trips AS
SELECT user_id,
  COUNT(trip_id) AS completed_trips,
  ROUND(SUM(
    CASE WHEN flight_booked AND return_flight_booked THEN 2
    WHEN flight_booked AND NOT return_flight_booked THEN 1
    ELSE 0 END),
  0) AS flights_completed,
  ROUND(AVG(checked_bags),1) AS avg_bags,
  ROUND(AVG(final_fare_usd),2) AS avg_fare,
  ROUND(AVG(fare_savings),2) AS avg_savings,
  ROUND(SUM(fare_savings),2) AS total_fare_savings,
  ROUND(AVG(booking_to_trip_days),0) AS avg_days_before_trip,
  ROUND(SUM(distance_flown_km)::NUMERIC,1) AS total_distance_flown_km,
  ROUND(AVG(flight_cost_per_km)::NUMERIC,4) AS avg_fare_per_km,
  ROUND(AVG(cost_saved_per_km)::NUMERIC,4) AS avg_fare_savings,
  ROUND(AVG(trip_length_days),1) AS avg_trip_length_days,
  ROUND(AVG(rooms),1) AS avg_rooms,
  ROUND(AVG(hotel_total_usd / NULLIF(rooms,0) / nights),2) AS avg_cost_per_room,
  ROUND(SUM(hotel_savings_per_room_usd),2) AS total_hotel_savings,
  ROUND(SUM(trip_total_usd),2) AS total_revenue
FROM completed_trips
GROUP BY user_id
;

-- QUERY 6: Create an aggregated temp table of application sessions grouped by user
CREATE TEMP TABLE user_agg_all_sessions AS
SELECT user_id,
  COUNT(DISTINCT session_id) AS sessions,
  ROUND(AVG(page_clicks),0) AS avg_page_clicks,
  ROUND(AVG(session_length_seconds),0) AS avg_session_length_seconds,
  COUNT(DISTINCT trip_id) AS trips_booked,
  COUNT(CASE WHEN cancellation = True THEN 1 END) AS cancelled_trips,
  ROUND(COUNT(
    CASE WHEN cancellation = True
    THEN 1 END)::NUMERIC /
    NULLIF(COUNT(DISTINCT trip_id),0),
  2) AS cancellation_rate
FROM filtered_sessions
GROUP BY user_id
;

-- QUERY 7: Return table of user behaviour
SELECT users.user_id,
   EXTRACT(YEAR FROM AGE('2023-07-02'::DATE, birthdate)) AS age,
   users.gender,
   CASE WHEN married THEN 1 ELSE 0 END AS is_married,
   CASE WHEN has_children THEN 1 ELSE 0 END AS has_children,
   users.home_country,
   users.home_city,
   users.home_airport,
   sessions.sessions,
   sessions.avg_page_clicks,
   sessions.avg_session_length_seconds,
   sessions.trips_booked,
   sessions.cancelled_trips,
   sessions.cancellation_rate,
   COALESCE(trips.completed_trips,0) AS completed_trips,
   trips.flights_completed,
   trips.avg_bags,
   trips.avg_fare,
   trips.avg_savings,
   trips.avg_days_before_trip,
   trips.total_distance_flown_km,
   trips.avg_fare_per_km,
   trips.avg_fare_savings,
   trips.total_fare_savings,
   trips.avg_trip_length_days,
   trips.avg_rooms,
   trips.avg_cost_per_room,
   trips.total_hotel_savings,
   trips.total_revenue
FROM user_agg_all_sessions AS sessions
LEFT JOIN users
  ON sessions.user_id = users.user_id
LEFT JOIN user_agg_completed_trips AS trips
  ON sessions.user_id = trips.user_id
;

-- QUERY 8: Drop temp tables after extracting CSV and running exploratory queries.
DROP TABLE filtered_sessions,
	completed_trips,
	user_agg_completed_trips,
	user_agg_all_sessions
;
    

