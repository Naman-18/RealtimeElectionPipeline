CREATE TABLE IF NOT EXISTS votes 
(
    voter_id VARCHAR(255) UNIQUE,
    candidate_id VARCHAR(255),
    voting_time TIMESTAMP,
    vote int DEFAULT 1,
    PRIMARY KEY (voter_id, candidate_id)
);