CREATE ROLE catalog LOGIN PASSWORD 'catalog-test-password';
CREATE DATABASE catalog OWNER catalog;
ALTER SYSTEM SET max_slot_wal_keep_size = '4GB';
