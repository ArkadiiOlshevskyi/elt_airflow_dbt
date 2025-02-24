-- cretaing basic table for min DAG

CREATE TABLE IF NOT EXISTS test_table (
    id SERIAL PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    latitude DECIMAL(9,6) NOT NULL,
    longitude DECIMAL(9,6) NOT NULL,
    message TEXT NOT NULL
);


-- check
SELECT * 
FROM test_table
LIMIT 10;
