-- ============================================================
-- Raw tables
-- ============================================================

CREATE TABLE IF NOT EXISTS raw_national_intensity (
    id              SERIAL PRIMARY KEY,
    from_date       TEXT NOT NULL,
    to_date         TEXT NOT NULL,
    forecast        INTEGER,
    actual          INTEGER,
    intensity_index TEXT,
    loaded_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw_generation (
    id              SERIAL PRIMARY KEY,
    from_date       TEXT NOT NULL,
    to_date         TEXT NOT NULL,
    fuel            TEXT NOT NULL,
    perc            REAL,
    loaded_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw_regional (
    id              SERIAL PRIMARY KEY,
    from_date       TEXT NOT NULL,
    to_date         TEXT NOT NULL,
    regionid        INTEGER NOT NULL,
    dnoregion       TEXT,
    shortname       TEXT,
    forecast        INTEGER,
    intensity_index TEXT,
    fuel            TEXT NOT NULL,
    perc            REAL,
    loaded_at       TIMESTAMP DEFAULT NOW()
);
