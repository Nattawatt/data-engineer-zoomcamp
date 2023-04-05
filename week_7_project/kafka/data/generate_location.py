from tqdm import tqdm
import json
from geopy.geocoders import Nominatim

def frange(start, stop, step):
    i = 0
    while start + i * step < stop:
        yield start + i * step
        i += 1

if __name__ == "__main__":
    # Define latitude and longitude bounds for Bangkok
    min_lat, max_lat = 13.60, 13.95
    min_lon, max_lon = 100.41, 100.98
    step = 0.01

    lat_lon_list = []

    for lat in frange(min_lat, max_lat, step):
        for lon in frange(min_lon, max_lon, step):
            lat_lon_list.append((lat, lon))

    locations = []
    geolocator = Nominatim(user_agent="test", timeout=10)

    for lat_long in tqdm(lat_lon_list):
        lat = lat_long[0]
        long = lat_long[1]
        location = geolocator.reverse(f"{lat}, {long}", language="en", exactly_one=True)
        address = location.raw['address']
        state = address.get('state', None)
        sub_district = address.get('quarter',None)
        district = address.get('suburb', None)

        if district != None and sub_district != None and state == "Bangkok":

            location_dict = {"lat": lat,
                            "long": long,
                            "sub_district": sub_district,
                            "district": district,
                            "state": state,
                            }
            
            locations.append(location_dict)
            print(location_dict)

    # Dump locations list in JSON format
    with open("locations.json", "w") as f:
        json.dump(locations, f, indent=2)