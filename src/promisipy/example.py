from promisipy import Promise, promisipy
import requests
from pprint import pprint


@promisipy(mode="multiprocessing")
def get_rnm_info_from_id(rnm_id: str) -> dict:
    result = requests.get(f"https://rickandmortyapi.com/api/character/{rnm_id}")
    data = result.json()
    name = data["name"]
    origin_url = data["origin"]["url"]
    location_url = data["location"]["url"]

    origin_promise = Promise(lambda: requests.get(origin_url).json()).start()
    location_promise = Promise(lambda: requests.get(location_url).json()).start()

    origin_resolution, location_resolution = Promise.all(
        [origin_promise, location_promise]
    )

    return {
        "id": rnm_id,
        "name": name,
        "origin": origin_resolution.result["name"],
        "location": location_resolution.result["name"],
    }


def main():
    promises = [get_rnm_info_from_id(i).start() for i in range(1, 100)]
    profiles = [profile_resultion.result for profile_resultion in Promise.all(promises)]
    pprint(profiles)


if __name__ == "__main__":
    main()
