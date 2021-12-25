import gamla
import requests
from timeit import default_timer as timer

added_actors = 0
for movie_id in range(5):
    start = timer()
    try:
        r = requests.get(
            "https://api.themoviedb.org/3/movie/"
            + str(movie_id)
            + "/credits?api_key=56bcaf59a960c93b3e74daf8b8c937f2"
        )
        if r.status_code != 200:
            continue
        actors_per_movie = gamla.pipe(
            r,
            lambda x: x.json(),
            gamla.itemgetter("cast"),
            gamla.filter(
                gamla.compose_left(
                    gamla.itemgetter("known_for_department"), gamla.equals("Acting")
                )
            ),
            gamla.map(gamla.itemgetter("id")),
            gamla.wrap_tuple,
            gamla.prefix(movie_id),
            gamla.explode(1),
            list,
        )
        print(actors_per_movie[:10])
        # for movie_and_actor in actors_per_movie:
        #     # cursor.execute(stmt, movie_and_actor)
        added_actors += len(actors_per_movie)
        if added_actors % 100 == 0:
            print("commiting till:", movie_id, "last one:", actors_per_movie)
            # cnx.commit()
            # print("finished commiting")
            print("--- %s seconds ---" % (timer() - start))
    except Exception as e:
        print("Error!", movie_id, "\n", e)

print(added_actors)
