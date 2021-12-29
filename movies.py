import requests
import gamla

from timeit import default_timer as timer

import utils

all_bechdels = gamla.pipe(
    "http://bechdeltest.com/api/v1/getAllMovies",
    requests.get,
    lambda x: x.json(),
    gamla.filter(gamla.compose_left(gamla.itemgetter("rating"), gamla.equals(3))),
    gamla.map(gamla.itemgetter("imdbid")),
    gamla.filter(gamla.identity),
    set,
)

null_if_zero = gamla.when(gamla.equals(0), gamla.just(None))

MOVIE_SPEC = {
    "movie_id": gamla.itemgetter("id"),
    "title": gamla.itemgetter("title"),
    "release_year": gamla.ternary(
        gamla.itemgetter("release_date"),
        gamla.compose_left(
            gamla.itemgetter("release_date"),
            gamla.take(4),
            "".join,
            int,
        ),
        gamla.just(None),
    ),
    "rating": gamla.itemgetter("vote_average"),
    "revenue": gamla.compose_left(gamla.itemgetter("revenue"), null_if_zero),
    "budget": gamla.compose_left(gamla.itemgetter("budget"), null_if_zero),
    "is_bechdel": gamla.compose_left(
        gamla.itemgetter("imdb_id"),
        gamla.drop(2),
        "".join,
        gamla.contains(all_bechdels),
        int,
    ),
}

cnx = utils.connect_to_db()
cursor = cnx.cursor()
added_movies = 0
stmt = (
    f"INSERT INTO movies ({', '.join(list(MOVIE_SPEC.keys()))}) "
    "VALUES (%s, %s, %s, %s, %s, %s, %s)"
)

start = timer()
for movie_id in range(1959, 10000):
    try:
        r = requests.get(
            "https://api.themoviedb.org/3/movie/"
            + str(movie_id)
            + "?api_key=56bcaf59a960c93b3e74daf8b8c937f2"
        )
        if r.status_code != 200:
            continue
        movie = gamla.pipe(
            r,
            lambda x: x.json(),
            gamla.apply_spec(MOVIE_SPEC),
        )
        try:
            cursor.execute(stmt, tuple(movie.values()))
        except:
            print("could not insert", tuple(movie.values()))
        added_movies += 1
        if added_movies % 100 == 0:
            print("commiting movies till:", movie_id, "last movie:", movie)
            cnx.commit()
            print("finished commiting")
            print("--- %s seconds ---" % (timer() - start))
            start = timer()
    except Exception as e:
        print("Error!", movie_id, "\n", e)

print(added_movies)
