import utils

cnx = utils.connect_to_db()
cursor = cnx.cursor()


def _run(filename, args):
    with open(filename, "r") as f:
        query = f.read()
    cursor.execute(query % args)
    return cursor.fetchall()


def most_feminist_actors(gender):
    """male/female/non-binary/not specified actors who participated in most bechdel test passing movies."""
    print(_run("queries/most_feminist_actors.txt", (gender,)))


def passing_movies_for_provider_in_era(from_year: int, to_year: int, provider: str):
    """All bechdel test passing movies in year range for provider. example for provider: "Netlix", "Hulu", "HBO Max"""
    print(
        _run(
            "queries/passing_movies_for_provider_in_era.txt",
            (from_year, to_year, provider),
        )
    )


def best_passing_movies_by_criterion(criterion: str):
    """Highest rating/budget/revenue bechdel test passing movies"""
    print(_run("queries/best_passing_movies_by_criterion.txt", (criterion,)))


def most_feminist_providers():
    """Providers with the most bechdel test passing movies."""
    print(_run("queries/most_feminist_providers.txt", ()))


def most_feminist_years():
    """Years in which the most bechdel test passing movies came out."""
    print(_run("queries/most_feminist_years.txt", ()))


def passing_movies_for_actor(actor_name: str):
    """All bechdel test passing movies for actor"""
    print(_run("queries/passing_movies_for_actor.txt", (actor_name,)))


def passing_overview_fulltext(text: str):
    """All movies that their overviews contain text. e.g: "based on a true story", "love story", "outer space"""
    print(_run("queries/passing_overview_fulltext.txt", (text,)))
