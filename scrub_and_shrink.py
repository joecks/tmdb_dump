import glob
import json
import gzip

from datetime import date

# this script requires the results of `tmdb.py`
# it shrinks the results to a reasonable size for TLRE demos (~50,000),
# by removing movies that:
#   are "Adult",
#   runtime is less than 1 hr,
#   don't have a poster,
#   don't have any votes

def scrub_chunks():
    """Collate a list of chunk paths into a single dictionary

    Keyword arguments:
    files -- list of paths to g-zipped chunks from `tmdb.py`
    """
    files = glob.glob('chunks/*')
    if len(files) == 0 :
        raise SystemExit("No chunks found in `chunks/`. Did you run `tmdb.py` already?")

    keep = dict()

    for f in files:
        if len(keep) < 1000000 :
            with gzip.open(f, "r") as zip_ref:
                movies = json.load(zip_ref)
                for m in movies.keys():
                    dat = movies[m]
                    if (
                        # not dat["adult"] and
                        # (dat["vote_count"] or 0) > 5 and
                        # (dat["budget"] or 0) > 1 and
                        # dat["original_language"] == "en" and
                        # dat["poster_path"] is not None and
                        # dat["runtime"] is not None and
                        # (dat["runtime"] or 0) > 30 and
                        dat["release_date"] and
                        dat["overview"] is not None and
                        len(dat["overview"]) > 50
                        ):
                        k = dat["id"]
                        # del dat["cast"]
                        keep.update({k : dat})
    return keep

if __name__ == "__main__":
    keep = scrub_chunks()
    print(len(keep))
    filename = "tmdb_dump_" + str(date.today()) + ".json"
    with open(filename, "w") as f:
        json.dump(keep, f)
