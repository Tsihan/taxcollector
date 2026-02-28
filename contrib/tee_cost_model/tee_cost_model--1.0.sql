/* tee_cost_model--1.0.sql */

SET client_min_messages = warning;
LOAD 'tee_cost_model';
RESET client_min_messages;

CREATE FUNCTION tee_cost_model_activate()
RETURNS boolean
AS 'MODULE_PATHNAME', 'tee_cost_model_activate'
LANGUAGE C PARALLEL SAFE STRICT;

SELECT tee_cost_model_activate();
