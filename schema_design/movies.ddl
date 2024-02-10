CREATE SCHEMA IF NOT EXISTS content;


CREATE TABLE IF NOT EXISTS content.film_work (
    id uuid PRIMARY KEY,
    title VARCHAR (200) NOT NULL,
    description TEXT NOT NULL,
    creation_date DATE NOT NULL,
    rating FLOAT,
    type VARCHAR(15) NOT NULL,
    created timestamp with time zone NOT NULL,
    modified timestamp with time zone NOT NULL
);


CREATE TABLE IF NOT EXISTS content.person (
    id uuid PRIMARY KEY,
    full_name VARCHAR(200) NOT NULL,
    created timestamp with time zone NOT NULL,
    modified timestamp with time zone NOT NULL
);


CREATE TABLE IF NOT EXISTS content.person_film_work (
    id uuid PRIMARY KEY,
    person_id uuid NOT NULL,
    film_work_id uuid NOT NULL,
    role VARCHAR(15),
    created timestamp with time zone NOT NULL,
    CONSTRAINT unique_person_film_work_role UNIQUE (film_work_id, person_id, role),
    FOREIGN KEY (person_id) REFERENCES content.person(id) ON DELETE CASCADE,
    FOREIGN KEY (film_work_id) REFERENCES content.film_work(id) ON DELETE CASCADE
);
ALTER TABLE content.person_film_work ADD CONSTRAINT unique_person_film_work_role UNIQUE (film_work_id, person_id, role);


CREATE INDEX CONCURRENTLY IF NOT EXISTS person_film_work_person_id
  ON content.person_film_work(person_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS person_film_work_film_work_id
  ON content.person_film_work(film_work_id);


CREATE TABLE IF NOT EXISTS content.genre (
    id uuid PRIMARY KEY,
    name VARCHAR (100) NOT NULL UNIQUE,
    description TEXT,
    created timestamp with time zone NOT NULL,
    modified timestamp with time zone NOT NULL
);


CREATE TABLE IF NOT EXISTS content.genre_film_work (
    id uuid PRIMARY KEY,
    genre_id uuid NOT NULL REFERENCES genre(id) ON DELETE CASCADE,
    film_work_id uuid NOT NULL REFERENCES film_work(id) ON DELETE CASCADE,
    created timestamp with time zone NOT NULL,
    UNIQUE (genre_id, film_work_id)
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS genre_film_work_genre_id
  ON content.genre_film_work(genre_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS genre_film_work_film_work_id
  ON content.genre_film_work(film_work_id);