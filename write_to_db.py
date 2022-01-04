import csv

import requests
import gamla

from timeit import default_timer as timer

import actors
import movie_actors
import movies
import overviews
import us_flatrate_watch_providers
import connection

problematics = set()
cnx = connection.connect_to_db()
cursor = cnx.cursor()
start = timer()
for id_ in range(40000):
    m, a, p, ma = [[], [], [], []]
    try:
        m = requests.get(movies.request_url(id_))
    except Exception as e:
        problematics.add(id_)
        print("Error!", id_, "\n", e)
    try:
        a = requests.get(actors.request_url(id_))
    except Exception as e:
        problematics.add(id_)
        print("Error!", id_, "\n", e)
    try:
        p = requests.get(us_flatrate_watch_providers.request_url(id_))
    except Exception as e:
        problematics.add(id_)
        print("Error!", id_, "\n", e)
    try:
        ma = requests.get(movie_actors.request_url(id_))
    except Exception as e:
        problematics.add(id_)
        print("Error!", id_, "\n", e)

    # For each API request, create the final values to be written and write them to DB.
    if m and m.status_code == 200:
        movie, overview = gamla.pipe(
            m,
            lambda x: x.json(),
            gamla.juxt(
                gamla.apply_spec(movies.MOVIE_SPEC),
                gamla.apply_spec(overviews.OVERVIEW_SPEC),
            ),
            tuple,
        )
        try:
            cursor.execute(movies.stmt, tuple(movie.values()))
            cursor.execute(overviews.stmt, tuple(overview.values()))
        except Exception as e:
            problematics.add(id_)
            print("could not insert movie or overview", id_, "error", e)

    if a and a.status_code == 200:
        actor = gamla.pipe(a, lambda x: x.json(), gamla.apply_spec(actors.ACTOR_SPEC))
        try:
            cursor.execute(actors.stmt, tuple(actor.values()))
        except Exception as e:
            problematics.add(id_)
            print("could not insert actor", id_, "error", e)
    if p and p.status_code == 200:
        providers_per_movie = gamla.pipe(
            p, us_flatrate_watch_providers.providers_per_movie(id_)
        )
        try:
            for movie_and_provider in providers_per_movie:
                cursor.execute(us_flatrate_watch_providers.stmt, movie_and_provider)
        except Exception as e:
            problematics.add(id_)
            print("could not insert movie and providers", id_, "error", e)
    if ma and ma.status_code == 200:
        actors_per_movie = gamla.pipe(ma, movie_actors.actors_per_movie(id_))
        try:
            for movie_and_actor in actors_per_movie:
                cursor.execute(movie_actors.stmt, movie_and_actor)
        except Exception as e:
            problematics.add(id_)
            print("could not insert movie and actors", id_, "error", e)
    # Every 100 ids, commit.
    if id_ % 100 == 0:
        print("commiting till:", id_)
        cnx.commit()
        print("finished commiting")
        print("--- %s seconds ---" % (timer() - start))
        start = timer()


print(f"done until {id_}!!!\nEncountered problems in:\n {problematics}")
