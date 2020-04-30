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
path_raw_shp_lines = path_raw_shp + '\\lines'
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
print("Проверить можно на этом сайте:")
print("https://osmnames.org/")
print("Пример ввода:Yaroslavl,Russia")
place = input()



which_res = 2
answer_region = "ok"
while answer_region != "y":
    if answer_region == "n":
        print("Введите значение заново:")
        place = input()
    try:
        gdf_poly = ox.gdf_from_place(place, which_result=which_res)
        print("Найденное значение:", gdf_poly.place_name[0])
        print("Найденное значение верное?y/n")
        answer_region = input()
    except (AttributeError):
        print("Вы ввели:",place)
        print("Введено некорректное значение, объект не найден")
        answer_region = "n"
# 

# which_result - параметр, который выбирает 1, 2 и тп найденные значения. 
# чаще всего, 1 - это точка (центр объекта), 2 - полигон (границы объекта)



while (isinstance(shapely.geometry.polygon.Polygon(), type(gdf_poly.geometry[0])) == False):
#     if(isinstance(shapely.geometry.polygon.Polygon(), type(gdf_poly.geometry[0])) == False):
    # if(isinstance(shapely.geometry.polygon.Polygon(), type(gdf_poly.geometry[0].centroid)) == False):
    print("Тип геометрии найденного объекта - не полигон, возможно, точка (центр объекта)")
    print("Был найден тип геометрии объекта под номером:",which_res)
    print("Будет произведен поиск другого типа геометрии для этого объекта")
    if which_res == 3:
        print("Тип геометрии 'полигон' не был найден для этого объекта, уточните объект")
        print("Выполнение скрипта будет остановлено")
        exit()
        break
    if which_res == 1:
        which_res = 3
    if which_res == 2:
        which_res = 1
        
    gdf_poly = ox.gdf_from_place(place, which_result=which_res)
    
# 
print("Тип геометрии найденного объекта - полигон")


print("Хотите задать буффер (плюс N метров вокруг полигона) для геометрии?y/n")
answ_buff = input()
if answ_buff == "y":
    reg = re.compile('[^0-9]')
    print("Введите число в метрах:")
    buff_dist = input()
    buff_dist = int(reg.sub('', buff_dist))
#     print(type(buff_dist), buff_dist)
    gdf_poly = ox.gdf_from_place(place, which_result=which_res, buffer_dist=buff_dist)
#



print("Хотите сохранить полигон в shp?y/n")
answ_poly = input()
if answ_poly == "y":
    gdf_poly.to_file('./data/raw/shp/poly/poly_{}_{}.shp'.format(place, str_date), encoding='utf-8')
#


# вот так выглядит ссылка для скачивания вручную:
# https://overpass-api.de/api/map?bbox=37.321,56.517,41.232,58.978
#это для определения, какие координаты за какую сторону света отвечают в ссылке
# west,south,east,north = 37.321,56.517,41.232,58.978 

url_new = str("https://overpass-api.de/api/map?bbox="
              +str(gdf_poly.bbox_west[0])+","
              +str(gdf_poly.bbox_south[0])+","
              +str(gdf_poly.bbox_east[0])+","
              +str(gdf_poly.bbox_north[0]))
print("Скачиваться будет отсюда:")
print(url_new)

print("Файл скачивается, подождите...")
# с помощью wget можно скачивать большие объемы, например, области
filename = wget.download(url_new, out='./data/raw/osm/map_{}_{}.osm'.format(place, str_date))
print()
print("Скачанный файл:")
print(filename)


# https://github.com/OSGeo/gdal/blob/master/gdal/data/osmconf.ini
# перед запуском сохрани в папку с этим скриптом файл osmconf.ini из ссылки выше

driver = ogr.GetDriverByName('OSM')
datasource = driver.Open(filename)
layer = datasource.GetLayer('lines')
if (isinstance(None, type(layer)) == True):
    print("Ошибка считывания")
# else:
    # print("Слой считан, все ок")

os.environ['SHAPE_ENCODING'] = "utf-8" #без этого сохраняемый шейп не поддреживает кириллицу, вместо нее - ???
drv = ogr.GetDriverByName('ESRI Shapefile')
new_name_shp = "./data/raw/shp/lines/lines_{}_{}.shp".format(place, str_date)
outds = drv.CreateDataSource(new_name_shp)
# outlyr = outds.CopyLayer(layer,'./shp/raw/shp/shp_{}_{}.shp'.format(place, str_date))
outlyr = outds.CopyLayer(layer, new_name_shp)
del outlyr,outds,layer #это нужно, чтобы файл закрылся, и данные в него записались
print("Сохраненный шейп:")
print(new_name_shp)
print("Некоторые строки обрезались (255  символов),")
print("поэтому полные строки будут сохранены в CSV")
print()

# сохранение слоя в DF, затем - в CSV
# без геометрии, так как она не сохранятеся таким способом
# это сохранение нужно для того, чтобы сохранить всю строку целиком в столбце other_tags
# т.к. при сохранении в shp она обрезается на 255 символах, а там часто - больше
# http://warsa.de/girs/tutorial/featuresdataframe.html#get-layer-as-pandas-dataframe

lrs = LayersReader(filename)
df = lrs.data_frame(1) # 1 - это слой lines
del lrs
# df.head(2)

new_df = pd.DataFrame(df).copy().reset_index() #drop - не надо, там многоуровневый header
new_df = new_df[['osm_id', 'name', 'highway', 'waterway', 'aerialway', 'barrier', 'man_made', 'other_tags']]
new_df.to_csv("./data/raw/csv/csv_{}_{}.csv".format(place, str_date), sep=";", encoding="utf-8-sig", index=False)
print("CSV сохранен")