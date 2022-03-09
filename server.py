from shapely.geometry import Point

from werkzeug.exceptions import HTTPException
from werkzeug.routing import FloatConverter, Map, Rule
from werkzeug.wrappers import Request, Response

from json import dumps

import pickle
import lzma

with lzma.open("shapes.bin.xz") as f:
    states, counties, notes = pickle.load(f)

def county_query(lon, lat):
    point = Point(lon, lat)
    f = lambda s: s[1].intersects(point)
    try:
        state = next(filter(f, states))[0]
    except StopIteration:
        return None

    result = {"state": state}
    try:
        county = next(filter(f, counties[state]))[0]
        result["name"] = county
    except StopIteration:
        pass

    note = notes.get(state)
    if note:
        result["note"] = note

    return result

def county_request(request, lat, lon):
    return Response(dumps(county_query(lon, lat), separators=(",", ":")), content_type="application/json")

class PatchedFloatConverter(FloatConverter):
    regex = r"\d+(\.\d+)?"

url_map = Map([Rule("/county/<float(signed=True):lat>/<float(signed=True):lon>", endpoint=county_request)], converters={"float": PatchedFloatConverter})

@Request.application
def application(request):
    adapter = url_map.bind_to_environ(request.environ)
    try:
        endpoint, values = adapter.match()
        return endpoint(request, **values)
    except HTTPException as e:
        return e

if __name__ == "__main__":
    from werkzeug.serving import run_simple
    run_simple("localhost", 8000, application)
