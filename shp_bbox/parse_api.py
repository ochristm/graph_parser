print()
print("________________________________________")
print()
print("Part  1. Parse data from OSM.")
from datetime import datetime
time_entry = "{:%H:%M:%S}".format(datetime.now())
print("time entry:", time_entry)
print("Please wait...")
print()

import os
import conda
conda_file_dir = conda.__file__
conda_dir = conda_file_dir.split('lib')[0]
proj_lib = os.path.join(conda_dir, 'Library\share')
# proj_lib = os.path.join(os.path.join(conda_dir, 'pkgs'), 'proj4-5.2.0-h6538335_1006\Library\share')
path_gdal = os.path.join(proj_lib, 'gdal')
os.environ ['PROJ_LIB']=proj_lib
os.environ ['GDAL_DATA']=path_gdal

# import networkx as nx
# import osmnx as ox
#import pandas as pd
import geopandas as gpd
import shapely
from shapely.geometry import LineString, MultiLineString, Polygon, Point

# import wget
# import gdal, ogr
# import momepy
from datetime import datetime
import re
# import girs
# from girs.feat.layers import LayersReader
import overpass
from pyproj import Proj, transform

#отключить предупреждения pandas (так быстрее считает!!!):
#pd.options.mode.chained_assignment = None

import warnings
warnings.filterwarnings("ignore")


import time
import requests as req
import urllib.request

################
def check_query():
    cnt=1
    while True:
        url_kill = 'http://overpass-api.de/api/kill_my_queries'
        response = urllib.request.urlopen(url_kill)
        url='http://overpass-api.de/api/status'
        resp = req.get(url)
        txt_resp = resp.text
        lst_str = txt_resp.split("\n")
        
        del response, resp
        if ('2 slots available now.' in lst_str) or ('1 slots available now.' in lst_str):
            break
        elif cnt == 3:
            print("no available slots in API")
            print("too long to wait, exit script")
            slt_time = []
            for lv in new_lst_str:
                if "Slot available after" in lv:
                    slt_time.append(lv)
            # 
            print(url)
            print(url_kill)
            print(slt_time)
            exit()
            break
        else:
            print("no available slots in API, try №:",cnt)
            print("wait...")
            cnt+=1
            time.sleep(30)
# 
################
check_query()

##############
def dataByPoly():
    filename = ''
    while (os.path.isfile(filename) != True):
        print("Введите путь и название shp с границами (polygon), пример:")
        print("./data/1701435/20200608_1650/raw/layers/poly_2_Ярославль_20200608_1650.shp")
        filename = input()
#         filename = '.\data' + filename.split(".\data")[1]
        if os.path.isfile(filename) == False:
            print("Файл не найден, попробуйте снова")
        else:
            break
    # 
    gdf_poly = gpd.read_file(filename)
    bounds = gdf_poly.geometry[0].bounds
    new_minlat = bounds[1]
    new_minlon = bounds[0]
    new_maxlat = bounds[3]
    new_maxlon = bounds[2]
    
    return gdf_poly,new_minlat,new_minlon,new_maxlat,new_maxlon

gdf_poly,new_minlat,new_minlon,new_maxlat,new_maxlon = dataByPoly()

api = overpass.API()

print("Введите название города/объекта")
place = input()
name_place = place
buff_km = 0

poly_osmid = sum(list(place.encode('utf-8'))) #len(place)
print("local place_id:",poly_osmid)

#########################

str_date = "{:%Y%m%d_%H%M}".format(datetime.now())

path_new = os.getcwd()
path_total = path_new + '\\data'
path_city = path_total + '\\' + str(poly_osmid)
path_data = path_city + '\\' + str_date
path_raw = path_data + '\\raw'
path_raw_osm = path_raw
path_raw_csv = path_raw
#path_raw_shp = path_raw + '\\shp'
path_raw_shp_layers = path_raw + '\\layers'
path_raw_shp_poly = path_raw_shp_layers

path_res = path_data + '\\res'
path_res_edges = path_res
path_res_nodes = path_res

list_paths = [path_total, path_city, path_data, path_raw, path_raw_shp_layers, path_res]

for path in list_paths:
    try:
        os.mkdir(path)
    except FileExistsError:
        pass
    except OSError:
        print ("Не удалось создать директорию: %s \n" % path)
    # else:
        # print ("Создана директория %s \n" % path)
# 

#########################


# import time
# import requests as req
# import urllib.request

# bbox=57.4464,39.2596,57.8528,40.4750
bbox=new_minlat,new_minlon,new_maxlat,new_maxlon

print("Getting links, restrictions")
print("Please, wait...")

check_query()

lst_highway_notok = ['steps', 'pedestrian', 'footway', 'path', 'raceway', 
                     'road', 'track', 'planned', 'proposed', 'cycleway']
new_str = '|'.join(lst_highway_notok)

ways = api.get(
"""[out:json][timeout:1800];
(

way["railway"~"."]{bbox};
way["barrier"="yes"]{bbox};
(
way["highway"~"."]{bbox};
-
way["highway"~"{new_str}"]{bbox};
);
relation["type"="restriction"]{bbox};
);
(._;>;);
out geom;""".format(bbox=bbox,new_str=new_str), 
build=False, responseformat="json")


print("Done")