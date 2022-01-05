CREATE TABLE IF NOT EXISTS movies (
	movie_id INT PRIMARY KEY,
    title VARCHAR(250) NOT NULL,
    release_year YEAR(4),
    rating FLOAT,
    revenue BIGINT(15),
    budget BIGINT(15),
    is_bechdel BIT
    );
    
CREATE TABLE IF NOT EXISTS movie_actors (
	movie_id INT,
    actor_id INT,
    PRIMARY KEY (movie_id, actor_id));

CREATE TABLE IF NOT EXISTS actors (
	actor_id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    gender VARCHAR(20));

CREATE TABLE IF NOT EXISTS us_flatrate_watch_providers (
	movie_id INT,
    name VARCHAR(100) NOT NULL,
    PRIMARY KEY (movie_id, name));

CREATE TABLE IF NOT EXISTS overviews (
	movie_id INT PRIMARY KEY,
    overview VARCHAR(2000));



-- indexes:
ALTER TABLE overviews ADD FULLTEXT(overview);
ALTER TABLE movies ADD INDEX(movie_id);
ALTER TABLE actors ADD INDEX(actor_id);

-- foreign keys:
ALTER TABLE movie_actors
ADD CONSTRAINT FK_movie
FOREIGN KEY (movie_id) REFERENCES movies(movie_id);

ALTER TABLE us_flatrate_watch_providers
ADD CONSTRAINT FK_providers
FOREIGN KEY (movie_id) REFERENCES movies(movie_id);

ALTER TABLE overviews
ADD CONSTRAINT FK_overviews
FOREIGN KEY (movie_id) REFERENCES movies(movie_id);






