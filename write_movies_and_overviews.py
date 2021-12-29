import requests
import gamla

from timeit import default_timer as timer

import movies
import overviews
import utils


cnx = utils.connect_to_db()
cursor = cnx.cursor()

start = timer()
for movie_id in range(5, 1000):
    try:
        m = requests.get(movies.request_url(movie_id))
        if m.status_code != 200:
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
        try:
            cursor.execute(movies.stmt, tuple(movie.values()))
            cursor.execute(overviews.stmt, tuple(overview.values()))
        except Exception as e:
            print("could not insert", movie_id, e)
        if movie_id % 100 == 0:
            print("commiting movies till:", movie_id, "last movie:", movie)
            cnx.commit()
            print("finished commiting")
            print("--- %s seconds ---" % (timer() - start))
            start = timer()
    except Exception as e:
        print("Error!", movie_id, "\n", e)

print("done!!!")
