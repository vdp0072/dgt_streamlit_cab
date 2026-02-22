-- Supabase helper SQL
-- Run these queries in Supabase SQL Editor to create a profiles table and seed an admin profile.

-- Create profiles table if not exists
CREATE TABLE IF NOT EXISTS profiles (
  id uuid PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
  email text,
  role text DEFAULT 'user',
  created_at timestamptz DEFAULT now()
);

-- Example: find a user's id by email (replace the email)
-- SELECT id, email FROM auth.users WHERE email = 'admin@example.com';

-- Example: insert or upsert an admin profile (replace <UUID> and email)
-- INSERT INTO profiles (id, email, role)
-- VALUES ('<UUID>', 'admin@example.com', 'admin')
-- ON CONFLICT (id) DO UPDATE SET role = EXCLUDED.role, email = EXCLUDED.email;
