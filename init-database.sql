CREATE USER metabase WITH PASSWORD 'P@ssw0rd';
CREATE DATABASE metabase OWNER metabase;
GRANT ALL PRIVILEGES ON DATABASE metabase TO metabase;
GRANT ALL PRIVILEGES ON DATABASE metabase TO "admin";