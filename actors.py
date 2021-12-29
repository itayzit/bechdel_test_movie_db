import gamla

ACTOR_SPEC = {
    "actor_id": gamla.itemgetter("id"),
    "name": gamla.itemgetter("name"),
    "gender": gamla.compose_left(
        gamla.itemgetter("gender"),
        gamla.case_dict(
            {
                gamla.equals(0): gamla.just("not specified"),
                gamla.equals(1): gamla.just("female"),
                gamla.equals(2): gamla.just("male"),
                gamla.equals(3): gamla.just("non binary"),
            }
        ),
    ),
}

stmt = (
    f"INSERT INTO actors ({', '.join(list(ACTOR_SPEC.keys()))}) " "VALUES (%s, %s, %s)"
)


def request_url(actor_id):
    return (
        "https://api.themoviedb.org/3/person/"
        + str(actor_id)
        + "?api_key=4dec3d85d9e68720edcce937fca2f95c"
    )
