import gamla
import requests
from timeit import default_timer as timer

# cnx = utils.connect_to_db()
# cursor = cnx.cursor()
stmt = (
    f"INSERT INTO us_flatrate_watch_providers (movie_id, provider_name) "
    "VALUES (%s, %s)"
)
added_providers = 0

for movie_id in range(100):
    start = timer()
    try:
        r = requests.get(
            "https://api.themoviedb.org/3/movie/"
            + str(movie_id)
            + "/watch/providers?api_key=56bcaf59a960c93b3e74daf8b8c937f2"
        )
        if r.status_code != 200:
            continue
        providers_per_movie = gamla.pipe(
            r,
            lambda x: x.json(),
            gamla.get_in(["results", "US", "flatrate"]),
            gamla.map(gamla.itemgetter("provider_name")),
            gamla.wrap_tuple,
            gamla.prefix(movie_id),
            gamla.explode(1),
            list,
        )
        print(providers_per_movie)
        # for movie_and_provider in providers_per_movie:
        #     # cursor.execute(stmt, movie_and_provider)
        added_providers += len(providers_per_movie)
        # if added_providers % 100 == 0:
        #     print("commiting providers till:", movie_id, "last one:", providers_per_movie)
        #     # cnx.commit()
        #     # print("finished commiting")
        #     print("--- %s seconds ---" % (timer() - start))
    except Exception as e:
        print("Error!", movie_id, "\n", e)
