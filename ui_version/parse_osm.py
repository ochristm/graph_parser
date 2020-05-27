print("Прежде чем запускать скрипт убедитесь, что выполнены все условия в readme.txt")
print("All ok?y/n")
answ_demand = input()

if answ_demand != "y":
    print("Условия не выполнены, скрипт завершится...")
    exit()
#

print()
print("________________________________________")
print()
print("Part  1. Parse data from OSM.")
print("Please wait...")
print()

import networkx as nx
import osmnx as ox
import pandas as pd
import geopandas as gpd
import shapely

import wget
import gdal, ogr
import os
import momepy
from datetime import datetime
import re
import girs
from girs.feat.layers import LayersReader
import overpass
from pyproj import Proj, transform

#отключить предупреждения pandas (так быстрее считает!!!):
pd.options.mode.chained_assignment = None

import warnings
warnings.filterwarnings("ignore")

###############################################

import os
path_new = os.getcwd()
path_data = path_new + '\\data'
path_raw = path_data + '\\raw'
path_raw_osm = path_raw + '\\osm'
path_raw_shp = path_raw + '\\shp'
path_raw_csv = path_raw + '\\csv'
path_raw_shp_lines = path_raw_shp + '\\layers'
path_raw_shp_poly = path_raw_shp + '\\poly'

path_res = path_data + '\\res'
path_res_edges = path_res + '\\edges'
path_res_nodes = path_res + '\\nodes'

list_paths = [path_data, path_raw, path_raw_osm, 
    path_raw_shp, path_raw_csv, path_raw_shp_lines, path_raw_shp_poly, 
    path_res, path_res_edges, path_res_nodes]

for path in list_paths:
    try:
        os.mkdir(path)
    except OSError:
        print ("Не удалось создать директорию: %s \n" % path)
        print("Возможно, она уже создана")
    else:
        print ("Создана директория %s \n" % path)
# 

###############################################
# place = 'Yaroslavl,Russia'
# Petropavlovsk-Kamchatsky
#здесь надо указать точное название населенного пункта, проверить, совпадает ли на осм
# проверить можно на этом сайте:
# https://osmnames.org/
str_date = "{:%Y%m%d_%H%M}".format(datetime.now())
print("Указать точное название населенного пункта, проверить, совпадает ли на OSM")

print("Пример ввода названия:Ярославль")
name_place=input()
print("Пример ввода типа:city")
type_place=input()
resp = {}
resp["elements"] = []
api = overpass.API()
while len(resp["elements"]) == 0:
    try:
        resp = api.get(
        """[out:json];
        area["ISO3166-1"="RU"][admin_level=6];
        relation["name"="{}"]["place"="{}"];
        out bb;""".format(name_place,type_place), 
        build=False, responseformat="json")
    except:
        resp["elements"] = []
    if len(resp["elements"]) != 0:
        dict_bbox = resp["elements"][0]["bounds"]
        print("Place is found")
        break
    else:
        print("Введено некорректное значение")
        print("Введено:",name_place, type_place)
        print("Введите заново:")
        print("Пример ввода названия:Ярославль")
        name_place=input()
        print("Пример ввода типа:city")
        type_place=input()
        resp = {}
        resp["elements"] = []
#

place = name_place
buffer = 0
buff_km = 0
print("Хотите задать буффер (плюс N метров вокруг полигона) для геометрии?y/n")
answ_buff = input()
if answ_buff == "y":
    reg = re.compile('[^0-9]')
    print("Введите число в метрах:")
    buffer = input()
    buffer = int(reg.sub('', buffer))
    buff_km = ((int(buffer/1000)) if (buffer % 1000 == 0) else round(buffer / 1000, 2))
    #gdf_poly = ox.gdf_from_place(place, which_result=which_res, buffer_dist=buff_dist)
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

# вот так выглядит ссылка для скачивания вручную:
# https://overpass-api.de/api/map?bbox=37.321,56.517,41.232,58.978
#это для определения, какие координаты за какую сторону света отвечают в ссылке
# west,south,east,north = 37.321,56.517,41.232,58.978 

url_new = str("https://overpass-api.de/api/map?bbox="
              +str(new_minlon)+","
              +str(new_minlat)+","
              +str(new_maxlon)+","
              +str(new_maxlat))
print("Скачиваться будет отсюда:")
print(url_new)

print("Файл скачивается, подождите...")
# с помощью wget можно скачивать большие объемы, например, области
filename = wget.download(url_new, out='./data/raw/osm/map_{}_{}_{}.osm'.format(buff_km, place, str_date))
print()
print("Скачанный файл:")
print(filename)

#################################
#get restrictions directly from api
import pandas as pd
# bbox=57.4464,39.2596,57.8528,40.4750
bbox=new_minlat,new_minlon,new_maxlat,new_maxlon

resrt = api.get(
"""[out:json];
relation{bbox}["type"="restriction"];
out body;""".format(bbox=bbox), 
build=False, responseformat="json")

######################
lst_one = []
i=0

for i in range(len(resrt['elements'])):
    one_el = resrt['elements'][i]
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

df = pd.DataFrame(data=lst_one, columns=['osm_id_restr', 'restr_type', 'role', 'osm_id_graph', 'geo_type'])

lst_way_via = list(df[((df.geo_type == 'way') & (df.role == 'via'))].osm_id_restr.unique())

df_ok_wnw = df[~df.osm_id_restr.isin(lst_way_via)].reset_index(drop=True)
df_notok_www = df[df.osm_id_restr.isin(lst_way_via)].reset_index(drop=True)

df_via_out = df_notok_www[~(df_notok_www.role == 'via')].reset_index(drop=True)

df_via_in = df_notok_www[df_notok_www.role == 'via'].reset_index(drop=True)
df_via_in_2 = df_via_in.copy()
df_via_in_2['role'] = '2from'
df_via_in['role'] = '1to'


df_from_in = df_notok_www[df_notok_www.role == 'from'].reset_index(drop=True)
df_from_in['role'] = '1from'
df_to_in = df_notok_www[df_notok_www.role == 'to'].reset_index(drop=True)
df_to_in['role'] = '2to'

df_new_via = df_from_in.append(df_via_in).append(df_via_in_2).append(df_to_in)
df_new_via = df_new_via.sort_values(by=['osm_id_restr', 'role']).reset_index(drop=True)

final_df_restr = df[((df.geo_type == 'way') & (~df.osm_id_restr.isin(lst_way_via)))]
final_df_restr = final_df_restr.append(df_new_via)
final_df_restr = final_df_restr.sort_values(by=['osm_id_restr','role']).reset_index(drop=True)
final_df_restr = final_df_restr.rename(columns={'osm_id_graph':'osm_id'})
final_df_restr['osm_id'] = final_df_restr['osm_id'].astype(str)

#################################
print("Done")