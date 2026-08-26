\set ON_ERROR_STOP on

SELECT format(
    'CREATE ROLE %I WITH LOGIN REPLICATION PASSWORD %L',
    :'cdc_user',
    :'cdc_password'
)
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_roles
    WHERE rolname = :'cdc_user'
)
\gexec

SELECT format(
    'ALTER ROLE %I WITH LOGIN REPLICATION PASSWORD %L',
    :'cdc_user',
    :'cdc_password'
)
\gexec

SELECT format(
    'GRANT CONNECT ON DATABASE %I TO %I',
    :'database_name',
    :'cdc_user'
)
\gexec

SELECT format(
    'GRANT USAGE ON SCHEMA public TO %I',
    :'cdc_user'
)
\gexec

SELECT format(
    'GRANT SELECT ON ALL TABLES IN SCHEMA public TO %I',
    :'cdc_user'
)
\gexec

SELECT format(
    'GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO %I',
    :'cdc_user'
)
\gexec

SELECT format(
    'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO %I',
    :'cdc_user'
)
\gexec

SELECT
    'CREATE PUBLICATION demandflow_publication FOR ALL TABLES'
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_publication
    WHERE pubname = 'demandflow_publication'
)
\gexec
