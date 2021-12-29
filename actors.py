import gamla
from timeit import default_timer as timer

import requests

import utils

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
cnx = utils.connect_to_db()
cursor = cnx.cursor()
added_actors = 0
stmt = (
    f"INSERT INTO actors ({', '.join(list(ACTOR_SPEC.keys()))}) " "VALUES (%s, %s, %s)"
)

for actor_id in range(5):
    start = timer()
    try:
        r = requests.get(
            "https://api.themoviedb.org/3/person/"
            + str(actor_id)
            + "?api_key=4dec3d85d9e68720edcce937fca2f95c"
        )
        if r.status_code != 200 or r.json()["known_for_department"] != "Acting":
            continue
        actor = gamla.pipe(
            r,
            lambda x: x.json(),
            gamla.apply_spec(ACTOR_SPEC),
        )
        print(actor)
        try:
            cursor.execute(stmt, tuple(actor.values()))
        except:
            print("could not insert", tuple(actor.values()))
        added_actors += 1
        if added_actors % 100 == 0:
            print("commiting actors till:", actor_id, "last one:", actor)
            cnx.commit()
            print("finished commiting")
            print("--- %s seconds ---" % (timer() - start))
    except Exception as e:
        print("Error!", actor_id, "\n", e)
