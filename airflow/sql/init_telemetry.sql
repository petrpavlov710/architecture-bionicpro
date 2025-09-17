CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE IF NOT EXISTS public.sensor_events (
  event_id BIGSERIAL PRIMARY KEY,
  device_id VARCHAR(64) NOT NULL,
  event_ts TIMESTAMP NOT NULL,
  battery_level NUMERIC(5,2),
  steps INT,
  load_kg NUMERIC(6,2)
);

-- Sample data for the last 3 days for devices
INSERT INTO public.sensor_events (device_id, event_ts, battery_level, steps, load_kg)
SELECT 'dev-001', NOW() - INTERVAL '1 day' * g, 100 - (g*5), 1000 + g*50, 10 + g
FROM generate_series(0, 2) g
UNION ALL
SELECT 'dev-002', NOW() - INTERVAL '1 day' * g, 90 - (g*3), 800 + g*30, 8 + g
FROM generate_series(0, 2) g
UNION ALL
SELECT 'dev-003', NOW() - INTERVAL '1 day' * g, 85 - (g*4), 1200 + g*60, 12 + g
FROM generate_series(0, 2) g;


