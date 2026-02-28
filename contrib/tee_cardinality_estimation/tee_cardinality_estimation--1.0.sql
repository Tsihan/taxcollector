-- tee_cardinality_estimation extension
-- Load the shared library so that GUCs and planner hooks register.
SET client_min_messages = warning;
LOAD 'tee_cardinality_estimation';
RESET client_min_messages;

CREATE FUNCTION tee_cardinality_estimation_activate()
RETURNS boolean
AS 'MODULE_PATHNAME', 'tee_cardinality_estimation_activate'
LANGUAGE C PARALLEL SAFE STRICT;

SELECT tee_cardinality_estimation_activate();