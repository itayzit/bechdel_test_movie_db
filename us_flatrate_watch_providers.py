import gamla

stmt = f"INSERT INTO us_flatrate_watch_providers (movie_id, name) " "VALUES (%s, %s)"


def providers_per_movie(movie_id):
    return gamla.compose_left(
        lambda x: x.json(),
        gamla.get_in_with_default(["results", "US", "flatrate"], []),
        gamla.map(gamla.itemgetter("provider_name")),
        gamla.wrap_tuple,
        gamla.prefix(movie_id),
        gamla.explode(1),
        list,
    )


def request_url(movie_id):
    return (
        "https://api.themoviedb.org/3/movie/"
        + str(movie_id)
        + "/watch/providers?api_key=56bcaf59a960c93b3e74daf8b8c937f2"
    )
