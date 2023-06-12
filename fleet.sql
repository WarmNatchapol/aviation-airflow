CREATE DATABASE fleet_db;

CREATE TABLE fleet (
    reg VARCHAR(6),
    icao24 CHAR(6),
    aircraft_type VARCHAR(50),
    aircraft_type_resign CHAR(4),
    aircraft_name VARCHAR(50),
    delivered DATE,
    age_years FLOAT(3),
    remark VARCHAR(20),
    PRIMARY KEY (reg)
);