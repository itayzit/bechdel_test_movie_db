import gamla
from timeit import default_timer as timer
import requests

OVERVIEW_SPEC = {
    "movie_id": gamla.itemgetter("id"),
    "overview": gamla.itemgetter("overview"),
}

# cnx = utils.connect_to_db()
# cursor = cnx.cursor()
stmt = (
    f"INSERT INTO overviews ({', '.join(list(OVERVIEW_SPEC.keys()))}) "
    "VALUES (%s, %s)"
)
added_overviews = 0
for movie_id in range(20):
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
        #     cursor.execute(stmt, tuple(overview.values()))
        # except:
        #     print("could not insert", tuple(overview.values()))
        # print(overview)
        added_overviews += 1
        if added_overviews % 100 == 0:
            print("commiting overviews till:", movie_id, "last one:", overview)
            # cnx.commit()
            # print("finished commiting")
            print("--- %s seconds ---" % (timer() - start))
    except Exception as e:
        print("Error!", movie_id, "\n", e)
