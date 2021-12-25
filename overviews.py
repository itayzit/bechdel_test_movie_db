import gamla
from timeit import default_timer as timer
import requests

OVERVIEW_SPEC = {
    "movie_id": gamla.itemgetter("id"),
    "overview": gamla.itemgetter("overview"),
}

# cnx = utils.connect_to_db()
# cursor = cnx.cursor()

added_overviews = 0
for movie_id in range(5):
    start = timer()
    try:
        r = requests.get(
            "https://api.themoviedb.org/3/movie/"
            + str(movie_id)
            + "?api_key=56bcaf59a960c93b3e74daf8b8c937f2"
        )
        if r.status_code != 200:
            continue
        overview = gamla.pipe(
            r,
            lambda x: x.json(),
            gamla.apply_spec(OVERVIEW_SPEC),
        )
        # try:
        #     cursor.execute(stmt, tuple(movie.values()))
        # except:
        #     print("could not insert", tuple(movie.values()))
        print(overview)
        added_overviews += 1
        if added_overviews % 100 == 0:
            print("commiting movies till:", movie_id, "last movie:", overview)
            # cnx.commit()
            # print("finished commiting")
            print("--- %s seconds ---" % (timer() - start))
    except Exception as e:
        print("Error!", movie_id, "\n", e)
