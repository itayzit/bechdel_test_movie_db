import requests
import gamla

from timeit import default_timer as timer

import actors
import movies
import overviews
import utils

problematics = set()
cnx = utils.connect_to_db()
cursor = cnx.cursor()

start = timer()
for movie_id in range(100, 200):
    try:
        m = requests.get(movies.request_url(movie_id))
        a = requests.get(actors.request_url(movie_id))
        if m.status_code != 200 or a.status_code != 200:
            continue
        movie, overview = gamla.pipe(
            m,
            lambda x: x.json(),
            gamla.juxt(
                gamla.apply_spec(movies.MOVIE_SPEC),
                gamla.apply_spec(overviews.OVERVIEW_SPEC),
            ),
            tuple,
        )
        actor = gamla.pipe(a, lambda x: x.json(), gamla.apply_spec(actors.ACTOR_SPEC))
        try:
            cursor.execute(movies.stmt, tuple(movie.values()))
            cursor.execute(overviews.stmt, tuple(overview.values()))
            cursor.execute(actors.stmt, tuple(actor.values()))
        except Exception as e:
            problematics.add(movie_id)
            print("could not insert", movie_id, "error", e)
        if movie_id % 10 == 0:
            print("commiting till:", movie_id, "last movie:", movie)
            cnx.commit()
            print("finished commiting")
            print("--- %s seconds ---" % (timer() - start))
            start = timer()
    except Exception as e:
        print("Error!", movie_id, "\n", e)

print(f"done until {movie_id}!!!\nEncountered problems in:\n {problematics}")
