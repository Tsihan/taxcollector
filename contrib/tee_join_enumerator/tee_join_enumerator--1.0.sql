/* tee_join_enumerator--1.0.sql */

SET client_min_messages = warning;
LOAD 'tee_join_enumerator';
RESET client_min_messages;

CREATE FUNCTION tee_join_enumerator_activate()
RETURNS boolean
AS 'MODULE_PATHNAME', 'tee_join_enumerator_activate'
LANGUAGE C PARALLEL SAFE STRICT;

SELECT tee_join_enumerator_activate();
