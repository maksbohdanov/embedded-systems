DROP TABLE IF EXISTS processed_agent_data;

CREATE TABLE processed_agent_data (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    road_state VARCHAR(255) NOT NULL,
    x FLOAT,
    y FLOAT,
    z FLOAT,
    latitude FLOAT,
    longitude FLOAT,
    timestamp TIMESTAMP
);
