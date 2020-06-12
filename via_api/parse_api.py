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
#import geopandas as gpd
# import shapely
# from shapely.geometry import LineString, MultiLineString, Polygon, Point

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

# api = overpass.API()

# minlat,minlon,maxlat,maxlon
# bbox=57.4553,39.2610,57.8433,40.4736

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

print("Указать точное название населенного пункта, проверить, совпадает ли на OSM")

print("Пример ввода названия:Ярославль")
name_place=input()
# print("Пример ввода типа:city")
# type_place=input()
resp = {}
resp["elements"] = []
api = overpass.API()
while len(resp["elements"]) == 0:
    resp = api.get(
    """[out:json][timeout:25];
    relation["name"="{}"];
    out bb;""".format(name_place), 
    build=False, responseformat="json") #area["ISO3166-1"="RU"][admin_level=6];
    type_resp = 'relation'
    if len(resp["elements"]) == 0:
        check_query()
        resp = api.get(
        """[out:json][timeout:25];
        way["name"="{}"];
        out bb;""".format(name_place), 
        build=False, responseformat="json")
        type_resp = 'way'
#
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
                    print("If not found - try way() or relation()")
                    print()
                    print("{}(123456);".format(type_resp))
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


print("Getting borders")
print("Please, wait...")

check_query()

#
poly_resp = api.get(
    """[out:json][timeout:25];
    {}({});
    (._;>;);
    out geom;""".format(type_resp,poly_osmid), 
    build=False, responseformat="json")
# 
print("Done")