-- Add up migration script here
CREATE TABLE
    IF NOT EXISTS teachers (
        id SERIAL PRIMARY KEY NOT NULL,
        name VARCHAR(255) NOT NULL
    );

CREATE TABLE
    IF NOT EXISTS schedules (
        id SERIAL PRIMARY KEY NOT NULL,
        teacher_id INT NOT NULL REFERENCES teachers (id),
        shift_group VARCHAR(255) NOT NULL,
        shift VARCHAR(255) NOT NULL,
        shift_type VARCHAR(255) NOT NULL,
        start_date TIMESTAMP NOT NULL,
        end_date TIMESTAMP NOT NULL
    );

CREATE TABLE
    IF NOT EXISTS invoices (
        id SERIAL PRIMARY KEY NOT NULL,
        teacher_name VARCHAR(255) NOT NULL,
        eligible BOOLEAN NOT NULL,
        activity_start TIMESTAMP NOT NULL,
        activity_end TIMESTAMP NOT NULL,
        shift VARCHAR(255) NOT NULL
    );