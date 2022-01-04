import gamla

stmt = f"INSERT INTO movie_actors (movie_id, actor_id) " "VALUES (%s, %s)"


def actors_per_movie(movie_id):
    return gamla.compose_left(
        lambda x: x.json(),
        gamla.itemgetter("cast"),
        gamla.filter(
            gamla.compose_left(
                gamla.itemgetter_or_none("known_for_department"), gamla.equals("Acting")
            )
        ),
        gamla.map(gamla.itemgetter("id")),
        gamla.wrap_tuple,
        gamla.prefix(movie_id),
        gamla.explode(1),
        list,
    )


def request_url(movie_id):
    return (
        "https://api.themoviedb.org/3/movie/"
        + str(movie_id)
        + "/credits?api_key=56bcaf59a960c93b3e74daf8b8c937f2"
    )
