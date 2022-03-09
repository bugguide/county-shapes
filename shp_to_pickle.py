from shapely import wkb
import shapely.geometry as geo

from shapefile import Reader
from zipfile import ZipFile
from pyproj import Transformer

import pickle
import lzma

# An example of how pickle can be dangerous:
# (This is done to help ensure compatibility across versions)
class Shape:
    def __init__(self, shape):
        self.shape = shape

    def __reduce__(self):
        return wkb.loads, (self.shape.wkb,)

def shapes(name, transformer=None):
    print(name)
    z = ZipFile(name + ".zip")
    shp = Reader(shp=z.open(name + ".shp"), dbf=z.open(name + ".dbf"), encoding="latin1" if name[4:] == "000a21a_e" else "utf8")
    shapes = shp.shapes()
    if transformer is not None:
        for s in shapes:
            s.points = [transformer.transform(*p)[::-1] for p in s.points]
    return zip(map(Shape, map(geo.shape, shapes)), shp.records())

dedup_map = {}

def dedup(value):
    if value in dedup_map:
        return dedup_map[value]
    dedup_map[value] = value
    return value

def main():
    transformer = Transformer.from_crs(3347, 4326)

    states = []
    counties = {}
    notes = {}

    states_abbrev = {}

    for (shape, record) in shapes("tl_2021_us_state"):
        if record[0] == "9":
            continue
        states_abbrev[record[2]] = dedup(record[5])
        counties[record[5]] = []
        states.append((record[5], shape))

    for (shape, record) in shapes("tl_2021_us_county"):
        if record[0][0] in {"6", "7"} or record[0] == "11":
            continue
        counties[states_abbrev[record[0]]].append((dedup(record[4]), shape))


    provinces_abbrev = {"10": "NL", "11": "PE", "12": "NS", "13": "NB", "24": "QC", "35": "ON", "46": "MB", "47": "SK", "48": "AB", "59": "BC", "60": "YT", "61": "NT", "62": "NU"}
    for province in provinces_abbrev.values():
        counties[province] = []
        if province not in {"NT", "NL"}:
            notes[province] = "Adapted from Statistics Canada, Boundary files, 2021."

    for (shape, record) in shapes("lpr_000a21a_e", transformer):
        states.append((provinces_abbrev[record[0]], shape))

    for (shape, record) in shapes("lcd_000a21a_e", transformer):
        if record[5] not in {"11", "12", "13", "35", "59", "62"}:
            continue
        counties[provinces_abbrev[record[5]]].append((dedup(record[2]), shape))

    for (shape, record) in shapes("lccs000a21a_e", transformer):
        if record[4] not in {"46", "47", "48"}:
            continue
        name = record[2]
        if record[4] == "47" and name != "Saskatoon":
            name = name[:name.index(" No. ")]
        counties[provinces_abbrev[record[4]]].append((dedup(name), shape))

    for (shape, record) in shapes("ler_000a21a_e", transformer):
        if record[4] != "24":
            continue
        counties[provinces_abbrev[record[4]]].append((dedup(record[2]), shape))

    for (shape, record) in shapes("BNDCFG_ENRITI_ADMIN", Transformer.from_crs("esri:102002", 4326)):
        counties["NT"].append((record[4], shape))
    notes["NT"] = "Adapted from NWT Geomatics boundary data."

    spm = next(filter(lambda s: s[1].WIKIDATAID.strip("\x00") == "Q34617", shapes("ne_10m_admin_0_countries")))
    states.append(("NL", spm[0]))
    counties["NL"].append((spm[1].NAME.strip("\x00"), spm[0]))

    with lzma.open("shapes.bin.xz", "w") as f:
        f.write(pickle.dumps((states, counties, notes)))


if __name__ == "__main__":
    main()
