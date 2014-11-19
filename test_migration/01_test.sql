-- +goose Up
CREATE TABLE test (
   ColA bigint PRIMARY KEY
);

INSERT INTO test VALUES (100);

