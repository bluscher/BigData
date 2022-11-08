--create etl user
CREATE USER etl WITH PASSWORD 'Miclave.1';
--grant connect
GRANT CONNECT ON DATABASE "product" TO etl;
--grant table permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO etl;