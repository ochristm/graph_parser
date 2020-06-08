print()
print("________________________________________")
print()
print("Part  1. Parse data from OSM.")
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

import networkx as nx
import osmnx as ox
import pandas as pd
import geopandas as gpd
import shapely
from shapely.geometry import LineString, MultiLineString, Polygon, Point

import wget
import gdal, ogr
import momepy
from datetime import datetime
import re
# import girs
# from girs.feat.layers import LayersReader
import overpass
from pyproj import Proj, transform

#отключить предупреждения pandas (так быстрее считает!!!):
pd.options.mode.chained_assignment = None

import warnings
warnings.filterwarnings("ignore")

# api = overpass.API()

# minlat,minlon,maxlat,maxlon
# bbox=57.4553,39.2610,57.8433,40.4736

import time
import requests as req
import urllib.request

url='http://overpass-api.de/api/status'
resp = req.get(url)
txt_resp = resp.text
lst_str = txt_resp.split("\n")
if '2 slots available now.' not in lst_str:
    url_kill = 'http://overpass-api.de/api/kill_my_queries'
    response = urllib.request.urlopen(url_kill)
    del response
    time.sleep(30)
#

print("Указать точное название населенного пункта, проверить, совпадает ли на OSM")

print("Пример ввода названия:Ярославль")
name_place=input()
# print("Пример ввода типа:city")
# type_place=input()
resp = {}
resp["elements"] = []
api = overpass.API()
while len(resp["elements"]) == 0:
    try:
        resp = api.get(
        """[out:json][timeout:25];
        relation["name"="{}"];
        out bb;""".format(name_place), 
        build=False, responseformat="json") #area["ISO3166-1"="RU"][admin_level=6];
    except:
        resp["elements"] = []
    if len(resp["elements"]) != 0:
        #############################
        if len(resp['elements']) > 1:
            lst_region = []
            lst_ind = []
            for i in range(len(resp['elements'])):
                region = str(i) + "_" + str(resp['elements'][i]['id'])
                try:
                    region = region + "_" + resp['elements'][i]['tags']['addr:region']
                except:
                    pass
                lst_region.append(region)
                lst_ind.append(i)
        # 
            print("Found several objects with such name in regions:")
            print("index_relationID_region(if in tags)")
            print()
            print(lst_region)
            while True:
                try:
                    print()
                    print("to check which one do you need, go to:")
                    print("https://overpass-turbo.eu/#")
                    print("Clear everything and insert this code (instead of 123456 use relationID)")
                    print()
                    print("relation(123456);")
                    print("(._;>;);")
                    print("out;")
                    print()
                    print("Then click 'Старт/Start', then - zoom (лупа)")
                    print()
                    print("Insert selected index (first number before '_')")
                    ind = int(input())
                    if ind in lst_ind:
                        break
                except:
                    print("That's not a valid option!")
            #
            
        # 
        else:
            ind = 0
        # 
        #############################
        dict_bbox = resp["elements"][ind]["bounds"]
        
        print("Place is found")
        break
    else:
        print("Введено некорректное значение")
        print("Введено:",name_place)
        print("Введите заново:")
        print("Пример ввода названия:Ярославль")
        name_place=input()
        # print("Пример ввода типа:city")
        # type_place=input()
        resp = {}
        resp["elements"] = []
#
poly_osmid = str(resp['elements'][ind]['id'])
#########################

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


place = name_place
buffer = 0
buff_km = 0
print("Хотите задать буффер (плюс N метров вокруг полигона) для геометрии?y/n")
answ_buff = input()
if answ_buff == "y":
    while True:
        try:
            reg = re.compile('[^0-9]')
            print("Введите число в метрах:")
            buffer = input()
            buffer = int(reg.sub('', buffer))
            if buffer:
                buff_km = ((int(buffer/1000)) if (buffer % 1000 == 0) else round(buffer / 1000, 2))
                break
        except:
            print("That's not a number!")
# 
buff_km = str(buff_km).replace(".","")
#################################


def add_buff(lat, lon, buffer, sign):
    inProj = Proj('epsg:4326', preserve_units=True)
    outProj = Proj('epsg:32637')
    
    new_lat, new_lon = transform(inProj,outProj, lat, lon)
    
    if sign == 'plus':
        new_lat, new_lon = new_lat+buffer, new_lon+buffer
    if sign == 'minus':
        new_lat, new_lon = new_lat-buffer, new_lon-buffer
    #
    new_lat, new_lon = transform(outProj,inProj, new_lat, new_lon)
    new_lat, new_lon = round(new_lat, 7), round(new_lon, 7)
    return new_lat, new_lon
#
if buffer > 0:
    new_minlat, new_minlon = add_buff(dict_bbox['minlat'], dict_bbox['minlon'], buffer, 'minus')
    new_maxlat, new_maxlon = add_buff(dict_bbox['maxlat'], dict_bbox['maxlon'], buffer, 'plus')
else:
    new_minlat, new_minlon = dict_bbox['minlat'], dict_bbox['minlon']
    new_maxlat, new_maxlon = dict_bbox['maxlat'], dict_bbox['maxlon']

#################################



# import time
# import requests as req
# import urllib.request

# bbox=57.4464,39.2596,57.8528,40.4750
bbox=new_minlat,new_minlon,new_maxlat,new_maxlon

print("Getting restrictions")
print("Please, wait...")
url='http://overpass-api.de/api/status'
resp = req.get(url)
txt_resp = resp.text
lst_str = txt_resp.split("\n")
if '2 slots available now.' not in lst_str:
    url_kill = 'http://overpass-api.de/api/kill_my_queries'
    response = urllib.request.urlopen(url_kill)
    del response
    time.sleep(30)
# 

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



big_lst=[]
lst_one = []
cnt=0
for element in ways['elements']:
    if element['type'] == 'way':
        lst_line = []
        for g in range(len(element['geometry'])):
            lon = element['geometry'][g]['lon']
            lat = element['geometry'][g]['lat']
            lst_line.append((lon, lat))
        line = LineString(lst_line)
        small_lst=[]
        small_lst.append(element['id'])
        try:
            small_lst.append(element['tags']['highway'])
        except:
            small_lst.append(None)
        try:
            str_tgs = str(element['tags'])
            str_tgs = str_tgs.replace("'",'"').replace('{','').replace('}','')
            str_tgs = str_tgs.replace('": "', '"=>"')
            small_lst.append(str_tgs)
        except:
            small_lst.append(None)
        small_lst.append(line)
        big_lst.append(small_lst)
    #
    if element['type'] == 'relation':
        one_el = element
        osm_id_restr = one_el['id']
        try:
            one_restr = one_el['tags']['restriction']
            all_tags = str(one_el['tags'])
            all_tags = all_tags.replace("'type': 'restriction'", "").replace("'restriction': ", "")
            all_tags = all_tags.replace(", }","}").replace("': '", ":")
            all_tags = all_tags.replace("{", "").replace("}", "").replace("'", "")
            for j in range(len(one_el['members'])):
                geo_type = one_el['members'][j]['type']
                osm_id_graph = one_el['members'][j]['ref']
                role = one_el['members'][j]['role']
                lst_one.append([osm_id_restr,all_tags,role,osm_id_graph,geo_type])
        except:
            #restriction:hgv
            pass
    #
    cnt+=1
#



gdf_lines = gpd.GeoDataFrame(data=big_lst, columns=['osm_id', 'highway', 'other_tags', 'geometry'])
gdf_lines.crs='epsg:4326'



df = pd.DataFrame(data=lst_one, columns=['osm_id_restr', 'restr_type', 'role', 'osm_id_graph', 'geo_type'])



print("Getting borders")
print("Please, wait...")
url='http://overpass-api.de/api/status'
resp = req.get(url)
txt_resp = resp.text
lst_str = txt_resp.split("\n")
if '2 slots available now.' not in lst_str:
    url_kill = 'http://overpass-api.de/api/kill_my_queries'
    response = urllib.request.urlopen(url_kill)
    del response
    time.sleep(30)
# 

poly_resp = api.get(
    """[out:json][timeout:25];
    relation({});
    (._;>;);
    out geom;""".format(poly_osmid), 
    build=False, responseformat="json")
# Collect coords into list

