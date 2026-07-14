CREATE TABLE powietrze_poziom2 (
    --klucz
    report_date DATE NOT NULL,
    station_type VARCHAR(64) NOT NULL,
    wind_sector VARCHAR(1) NOT NULL,

    --miara 1
    pm25_sum DOUBLE NOT NULL,
    pm25_count BIGINT NOT NULL,
    avg_pm25 DOUBLE NOT NULL,

    --miara 2
    boundary_layer_sum DOUBLE NOT NULL,
    boundary_layer_count BIGINT NOT NULL,
    avg_boundary_layer_height_m DOUBLE NOT NULL,

    -- miara 3 moze byc laczona z innymi dobami dlatego jako text
    station_hll_state TEXT NOT NULL,
    station_count_estimated BIGINT NOT NULL,

    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
       ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (report_date, station_type, wind_sector)
);