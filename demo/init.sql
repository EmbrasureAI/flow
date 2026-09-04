CREATE ROLE catalog LOGIN PASSWORD 'catalog-test-password';
CREATE DATABASE catalog OWNER catalog;

CREATE TABLE public.orders (id bigint PRIMARY KEY, status text);
ALTER TABLE public.orders REPLICA IDENTITY FULL;
CREATE PUBLICATION embrasure_flow FOR TABLE public.orders;
INSERT INTO public.orders VALUES (1, 'created');
