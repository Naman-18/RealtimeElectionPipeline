CREATE TABLE IF NOT EXISTS voters 
(
    voter_id VARCHAR(255) PRIMARY KEY,
    voter_name VARCHAR(255),
    date_of_birth VARCHAR(255),
    gender VARCHAR(255),
    nationality VARCHAR(255),
    registration_number VARCHAR(255),
    address_street VARCHAR(255),
    address_city VARCHAR(255),
    address_state VARCHAR(255),
    address_country VARCHAR(255),
    address_postcode VARCHAR(255),
    email VARCHAR(255),
    phone_number VARCHAR(255),
    cell_number VARCHAR(255),
    picture TEXT,
    registered_age INTEGER
);