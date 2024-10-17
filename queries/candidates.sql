CREATE TABLE IF NOT EXISTS candidates 
(
    candidate_id VARCHAR(255) PRIMARY KEY,
    candidate_name VARCHAR(255),
    party_affiliation VARCHAR(255),
    date_of_birth VARCHAR(255),
    biography TEXT,
    campaign_platform TEXT,
    photo_url TEXT
);