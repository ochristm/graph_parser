# print("Прежде чем запускать скрипт убедитесь, что выполнены все условия в readme.txt")
# print("All ok?y/n")
# answ_demand = input()

# if answ_demand != "y":
    # print("Условия не выполнены, скрипт завершится...")
    # exit()
# #

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

# # В случе ошибки RuntimeError: b'no arguments in initialization list'
# # Если действие выше не помогло, то нужно задать системной переменной PROJ_LIB
# # явный путь к окружению по аналогии ниже
# Для настройки проекции координат, поменять на свой вариант

# os.environ ['PROJ_LIB']=r'C:\Users\popova_kv\AppData\Local\Continuum\anaconda3\Library\share'
# os.environ ['GDAL_DATA']=r'C:\Users\popova_kv\AppData\Local\Continuum\anaconda3\Library\share\gdal'

import networkx as nx
import osmnx as ox
import pandas as pd
import geopandas as gpd
import shapely

import wget
import gdal, ogr
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

###############################################
# place = 'Yaroslavl,Russia'
# Petropavlovsk-Kamchatsky
#здесь надо указать точное название населенного пункта, проверить, совпадает ли на осм
# проверить можно на этом сайте:
# https://osmnames.org/

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
try:
    filename = wget.download(url_new, out='{}\\map_{}_{}_{}.osm'.format(path_raw_osm,buff_km, place, str_date), bar=None)
except:
    print("HTTP Error 429: Too Many Requests")
    print("Try again later, and go:")
    print()
    print("http://overpass-api.de/api/status")
    print()
    print("http://overpass-api.de/api/kill_my_queries")
    exit()
print()
print("Скачанный файл:")
str_filename = '.\\data'+ filename.split('data')[1]
print('\t',str_filename)

#################################
#get restrictions directly from api
import pandas as pd
import time
import requests as req
import urllib.request

# bbox=57.4464,39.2596,57.8528,40.4750
bbox=new_minlat,new_minlon,new_maxlat,new_maxlon

print("Getting restrictions")
print("Please, wait...")

check_query()

resrt = api.get(
"""[out:json][timeout:25];
relation{bbox}["type"="restriction"];
out;""".format(bbox=bbox), 
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