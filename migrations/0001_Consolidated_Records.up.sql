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
        date DATE NOT NULL,
        shift VARCHAR(255) NOT NULL,
        shift_group VARCHAR(255) NOT NULL,
        shift_type VARCHAR(255) NOT NULL
    );