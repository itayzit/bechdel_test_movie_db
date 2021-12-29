import requests
import gamla


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

stmt = (
    f"INSERT INTO movies ({', '.join(list(MOVIE_SPEC.keys()))}) "
    "VALUES (%s, %s, %s, %s, %s, %s, %s)"
)


def request_url(movie_id):
    return (
        "https://api.themoviedb.org/3/movie/"
        + str(movie_id)
        + "?api_key=56bcaf59a960c93b3e74daf8b8c937f2"
    )
