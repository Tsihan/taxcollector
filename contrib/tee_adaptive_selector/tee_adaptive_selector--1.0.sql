/* tee_adaptive_selector--1.0.sql */

SET client_min_messages = warning;
LOAD 'tee_adaptive_selector';
RESET client_min_messages;

CREATE FUNCTION tee_adaptive_selector_activate()
RETURNS boolean
AS 'MODULE_PATHNAME', 'tee_adaptive_selector_activate'
LANGUAGE C PARALLEL SAFE STRICT;

SELECT tee_adaptive_selector_activate();
