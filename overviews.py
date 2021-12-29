import gamla


OVERVIEW_SPEC = {
    "movie_id": gamla.itemgetter("id"),
    "overview": gamla.itemgetter("overview"),
}

stmt = (
    f"INSERT INTO overviews ({', '.join(list(OVERVIEW_SPEC.keys()))}) "
    "VALUES (%s, %s)"
)
