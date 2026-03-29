CREATE TABLE IF NOT EXISTS dim_fuel (
    fuel_id SERIAL PRIMARY KEY,
    fuel VARCHAR(50) UNIQUE NOT NULL,
    category VARCHAR(50) NOT NULL
);

INSERT INTO dim_fuel (fuel, category) VALUES
('coal', 'fossil'),
('gas', 'fossil'),
('nuclear', 'non-fossil'),
('hydro', 'renewable'),
('wind', 'renewable'),
('solar', 'renewable'),
('biomass', 'renewable'),
('other', 'other'),
('imports', 'other')
ON CONFLICT (fuel) DO NOTHING;

CREATE TABLE IF NOT EXISTS dim_region (
    region_id SERIAL PRIMARY KEY,
    api_region_id INTEGER UNIQUE,
    dnoregion VARCHAR(100) UNIQUE NOT NULL,
    shortname VARCHAR(50) UNIQUE NOT NULL
);

INSERT INTO dim_region (api_region_id, dnoregion, shortname) VALUES
(0, 'National Grid', 'National Grid'),
(1, 'Scottish Hydro Electric Power Distribution', 'North Scotland'),
(2, 'SP Distribution', 'South Scotland'),
(3, 'Electricity North West', 'North West England'),
(4, 'NPG North East', 'North East England'),
(5, 'NPG Yorkshire', 'Yorkshire'),
(6, 'SP Manweb', 'North Wales & Merseyside'),
(7, 'WPD South Wales', 'South Wales'),
(8, 'WPD West Midlands', 'West Midlands'),
(9, 'WPD East Midlands', 'East Midlands'),
(10, 'UKPN East', 'East England'),
(11, 'WPD South West', 'South West England'),
(12, 'SSE South', 'South England'),
(13, 'UKPN London', 'London'),
(14, 'UKPN South East', 'South East England'),
(15, 'England', 'England'),
(16, 'Scotland', 'Scotland'),
(17, 'Wales', 'Wales'),
(18, 'GB', 'GB')
ON CONFLICT (dnoregion) DO NOTHING;

CREATE TABLE IF NOT EXISTS fact_intensity (
    id SERIAL PRIMARY KEY,
    from_date TIMESTAMP NOT NULL,
    to_date TIMESTAMP NOT NULL,
    region_id INTEGER NOT NULL,
    forecast INTEGER NOT NULL,
    actual INTEGER,
    intensity_index VARCHAR(50),
    source VARCHAR(50) NOT NULL,
    FOREIGN KEY (region_id) REFERENCES dim_region(region_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_intensity_period
ON fact_intensity (from_date, to_date);

CREATE TABLE IF NOT EXISTS fact_generation (
    id SERIAL PRIMARY KEY,
    from_date TIMESTAMP NOT NULL,
    to_date TIMESTAMP NOT NULL,
    region_id INTEGER NOT NULL,
    fuel_id INTEGER NOT NULL,
    perc REAL NOT NULL,
    source VARCHAR(50) NOT NULL,
    FOREIGN KEY (region_id) REFERENCES dim_region(region_id),
    FOREIGN KEY (fuel_id) REFERENCES dim_fuel(fuel_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_generation_period
ON fact_generation (from_date, to_date);
