CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE IF NOT EXISTS public.customers (
  customer_id SERIAL PRIMARY KEY,
  device_id VARCHAR(64) UNIQUE NOT NULL,
  email VARCHAR(255) NOT NULL,
  first_name VARCHAR(100) NOT NULL,
  last_name VARCHAR(100) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

INSERT INTO public.customers (device_id, email, first_name, last_name)
VALUES
  ('dev-001', 'user1@example.com', 'User', 'One'),
  ('dev-002', 'user2@example.com', 'User', 'Two'),
  ('dev-003', 'prothetic1@example.com', 'Prothetic', 'One')
ON CONFLICT (device_id) DO NOTHING;


