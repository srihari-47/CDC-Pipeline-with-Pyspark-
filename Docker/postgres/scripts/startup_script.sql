--Creating and loading the data to tables


CREATE TABLE flights(
flight_id SMALLINT PRIMARY KEY,
flight_no CHAR(6) NOT NULL,
departure_station CHAR(3) NOT NULL,
arrival_station CHAR(3) NOT NULL,
departure_time_utc TIMESTAMP NOT NULL,
arrival_time_utc TIMESTAMP NOT NULL);


CREATE TABLE customers(
Customer_ID INT PRIMARY KEY,
first_name VARCHAR(30) NOT NULL,
middle_name VARCHAR(30),
last_name VARCHAR(30) NOT NULL,
full_name VARCHAR(100) NOT NULL,
email VARCHAR(40) NOT NULL,
created_at TIMESTAMP);

CREATE TABLE bookings(
Booking_id INT PRIMARY KEY,
Customer_id INT REFERENCES CUSTOMERS(CUSTOMER_ID),
flight_id SMALLINT REFERENCES FLIGHTS(FLIGHT_ID),
created_at TIMESTAMP,
updated_at TIMESTAMP);

COPY FLIGHTS(flight_id, flight_no, departure_station, arrival_station, departure_time_utc, arrival_time_utc)
FROM '/table_data/flights.csv'
DELIMITER ','
CSV HEADER;

COPY CUSTOMERS(Customer_ID,first_name,middle_name,last_name,full_name,email,created_at)
FROM '/table_data/customer.csv'
DELIMITER ','
CSV HEADER;


COPY bookings(Booking_id,Customer_id,flight_id,created_at,updated_at)
FROM '/table_data/bookings.csv'
DELIMITER ','
CSV HEADER;


--Create a role 'debezium' with replication privilege
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'debezium';

--Create a replication_group and add users postgres & debezium to it. Alter ownership of our tables to replication_group so that ownership is shared.
CREATE ROLE rep_group;
GRANT rep_group TO postgres;
GRANT rep_group TO debezium;
ALTER TABLE bookings OWNER TO rep_group;
ALTER TABLE flights OWNER TO rep_group;
ALTER TABLE flights OWNER TO rep_group;

--Make the REPLICA IDENTITY AS FULL
ALTER TABLE flights REPLICA IDENTITY FULL;
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE bookings REPLICA IDENTITY FULL;

--GRANT CONNECT,SELECT & CREATE access to debezium user.
GRANT CONNECT ON DATABASE airline TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
GRANT CREATE ON SCHEMA public TO debezium;


--Manually creating publication as 'postgres' user. For some reason, If i try to create publication as 'debezium' it is giving an error.
CREATE PUBLICATION dbz_publication FOR TABLE public.flights, public.customers, public.bookings;


