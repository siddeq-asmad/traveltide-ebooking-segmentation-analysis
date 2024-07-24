/*
Time Cohort:
- Sessions after 2023-01-04 (after the new year holiday)

Behaviour Cohort:
- Users that had more than 7 sessions during that period
	OR users that booked at least 5 trips during that period
*/

-- Filter to sessions after 4th Jan 2023
WITH sessions_2023 AS (
    SELECT *
    FROM sessions
    WHERE DATE_TRUNC('day', session_start) > '2023-01-04'
),

/*
Get user_ids with more than 7 sessions
OR booked at least 5 trips after 4th Jan 2023
*/
  active_users_2023 AS (
    SELECT user_id
    FROM sessions_2023
    GROUP BY user_id
    HAVING COUNT(session_id) > 7
    	OR COUNT(DISTINCT trip_id) > 1
    ORDER BY user_id
),

/*
Create a filtered sessions table by inner joining our active users 
to the sessions_2023. Join hotels and flights to our filtered sessions table
selecting only non duplicated columns after the joins
Extract the epoch time, which is the number of seconds since the Unix epoch (00:00:00 UTC on 1 January 1970)
Applied discounts to price columns to calculate the final price.
Some stays were not cancelled through our app, this is indicated by a negative nights 
value (e.g. -1 or -2 nights). These stays incur a cancellation charge of 1 night.
*/
  filtered_sessions AS (
    SELECT sessions.*,
			ROUND(EXTRACT(EPOCH FROM session_end - session_start),0) AS session_length_seconds,
      hotel_name,
      CASE WHEN nights < 1 THEN 1 ELSE nights END AS nights,
      hotels.rooms,
      hotels.check_in_time,
      hotels.check_out_time,
      hotels.hotel_per_room_usd * (1 - COALESCE(hotel_discount_amount,0)) AS final_hotel_per_room_usd,
      hotels.hotel_per_room_usd * COALESCE(hotel_discount_amount,0) AS hotel_savings_per_room_usd,
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
      flights.base_fare_usd * (1 - COALESCE(flight_discount_amount,0)) AS final_fare_usd,
      flights.base_fare_usd * COALESCE(flight_discount_amount,0) AS fare_savings
    FROM sessions_2023 AS sessions
    INNER JOIN active_users_2023 AS users
    	ON sessions.user_id = users.user_id
    LEFT JOIN hotels
    	ON sessions.trip_id = hotels.trip_id
    LEFT JOIN flights
    	ON sessions.trip_id = flights.trip_id
),

-- Trip_ids of cancelled trips
  cancelled_trips AS (
    SELECT trip_id AS cancel_id
    FROM filtered_sessions
    WHERE cancellation = True
),
	-- Trips that don't have a cancel_id from the previous CTE
  completed_trips AS (
    SELECT *
    FROM filtered_sessions AS sessions
    LEFT JOIN cancelled_trips AS cancelled
    	ON sessions.trip_id = cancelled.cancel_id
    WHERE trip_id IS NOT NULL
    	AND cancel_id IS NULL
),

/*
Time from booking session to start of trip
Calculated time between the booking session and the start of the trip
flight with no hotel = session -> flight departure
flight before hotel = session -> flight departure
hotel stay before flying home = session-> hotel check-in
hotel with no flight = session -> hotel check-in
Haversine formula, calculate the distance between two locations on a sphere (Earth).
*/
  completed_trips_initial_metrics AS (
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
      -- Calculate the trip length depending on different booking combinations
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
    FROM completed_trips AS trips
    JOIN users
    	ON trips.user_id = users.user_id
),

	completed_trip_metrics_ratios AS (
    SELECT *,
      CASE WHEN distance_flown_km > 0 THEN flight_total_usd / distance_flown_km 
        ELSE NULL END AS flight_cost_per_km,
      CASE WHEN distance_flown_km > 0 THEN fare_savings / distance_flown_km 
        ELSE NULL END AS cost_saved_per_km
    FROM completed_trips_initial_metrics
),

	-- User level completed trips
  user_aggregated_completed_trips AS (
    SELECT user_id,
      COUNT(trip_id) AS completed_trips,
      ROUND(SUM(
      CASE WHEN flight_booked AND return_flight_booked
      THEN 2
      WHEN flight_booked AND NOT return_flight_booked
      THEN 1
      ELSE 0
      END),
      0) AS flights_completed,
      ROUND(AVG(checked_bags),1) AS avg_bags,
      ROUND(AVG(final_fare_usd),2) AS avg_fare,
      ROUND(AVG(fare_savings),2) AS avg_savings,
      ROUND(SUM(fare_savings),2) AS total_fare_savings,
      ROUND(AVG(booking_to_trip_days),1) AS avg_days_before_trip,
      ROUND(SUM(distance_flown_km)::NUMERIC,1) AS total_distance_flown_km,
      ROUND(AVG(flight_cost_per_km)::NUMERIC,4) AS avg_fare_per_km,
      ROUND(AVG(cost_saved_per_km)::NUMERIC,4) AS avg_fare_savings,
      ROUND(AVG(trip_length_days),1) AS avg_trip_length_days,
      ROUND(AVG(rooms),1) AS avg_rooms,
      ROUND(AVG(hotel_total_usd / NULLIF(rooms,0) / nights),2) AS avg_cost_per_room,
      ROUND(SUM(hotel_savings_per_room_usd),2) AS total_hotel_savings,
      ROUND(SUM(trip_total_usd),2) AS total_revenue
    FROM completed_trip_metrics_ratios
    GROUP BY user_id
),

	-- User level all sessions
	user_aggregated_all_sessions AS (
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
),

	user_table AS (
    SELECT users.user_id,
    -- 2nd July is the middle of the year for non leap years
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
    FROM user_aggregated_all_sessions AS sessions
    LEFT JOIN users
    	ON sessions.user_id = users.user_id
    LEFT JOIN user_aggregated_completed_trips AS trips
    	ON sessions.user_id = trips.user_id
),
    
	ranked_rev AS (
    SELECT DENSE_RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank,
      user_id,
      total_revenue,
      total_hotel_savings,
      total_fare_savings,
      total_revenue / completed_trips AS avg_revenue,
      completed_trips,
      SUM(total_revenue) OVER (ORDER BY total_revenue DESC) AS rolling_sum,
      SUM(total_revenue) OVER () AS total_sum
    FROM user_table
    WHERE total_revenue IS NOT NULL
),

  rolling_rev AS ( 
    SELECT user_id,
      total_revenue,
      SUM(total_revenue) OVER (ORDER BY total_revenue DESC) AS rolling_sum,
      SUM(total_revenue) OVER () AS all_revenue
    FROM user_table
    WHERE total_revenue IS NOT NULL
),
 
  segments AS (
    SELECT *,
      CASE WHEN total_revenue > 10000 THEN 'Luxurious'
        WHEN flights_completed > 0 AND avg_bags < 1 AND completed_trips > 3 THEN 'Business'
        WHEN flights_completed > 0 AND avg_bags > 1 OR avg_rooms > 1 THEN 'Family/Group'
        WHEN avg_savings > 0 THEN 'Bargain Hunter'
        WHEN completed_trips = 0 THEN 'Dreamer'
        ELSE 'Standard'
      END AS segment
    FROM user_table
)
 
SELECT segment,
  COUNT(*) AS customers,
  ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM segments),1) AS perc_of_total,
  SUM(total_revenue) AS total_revenue,
  ROUND(SUM(total_revenue) * 100.0 / (SELECT SUM(total_revenue) FROM segments),1) AS perc_of_total_revenue	
FROM segments
GROUP BY segment
;
