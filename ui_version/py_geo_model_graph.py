print("Прежде чем запускать скрипт убедитесь, что выполнены все условия в readme.txt")
print("All ok?y/n")
answ_demand = input()

if answ_demand != "y":
	print("Условия не выполнены, скрипт завершится...")
	exit()
#

import pandas as pd
import numpy as np
from datetime import datetime
import geopandas as gpd
import shapely as shp

import os
from tqdm.notebook import tqdm

# # В случе ошибки RuntimeError: b'no arguments in initialization list'
# # необходимо снять комментарии у этого текста
# conda_file_dir = conda.__file__
# conda_dir = conda_file_dir.split('lib')[0]
# proj_lib = os.path.join(os.path.join(conda_dir, 'pkgs'), 'proj4-5.2.0-h6538335_1006\Library\share')
# os.environ["PROJ_LIB"] = proj_lib

# # Если действие выше не помогло, то нужно задать системной переменной PROJ_LIB
# # явный путь к окружению по аналогии ниже
# Для настройки проекции координат, поменять на свой вариант
os.environ ['PROJ_LIB']=r'C:\Users\popova_kv\AppData\Local\Continuum\anaconda3\Library\share'

#отключить предупреждения pandas (так быстрее считает!!!):
pd.options.mode.chained_assignment = None

from shapely import wkt
from shapely.wkt import loads
from shapely.geometry import Point, LineString, mapping, shape
from shapely.ops import unary_union

import networkx as nx
import momepy


print()
print("________________________________________")
print()
print("Первая часть - скачивание и сохранение графа")
print()

###########################################
import py_read_api
###########################################
print()
print("________________________________________")
print()
print("Вторая часть - обработка графа")
print()

new_name_shp = py_read_api.new_name_shp
place = py_read_api.place
str_date = py_read_api.str_date
boundary = py_read_api.gdf_poly

read_shp = gpd.read_file(new_name_shp, encoding = 'utf-8', errors='ignore')
city_graph = read_shp.copy()

# пересечение ребер с границей выбранного участка (изначально граф выгружается как прямоугольник)
city_graph = gpd.sjoin(city_graph, boundary[['geometry']], 
	how='inner', op='intersects').drop("index_right", axis=1).reset_index(drop=True)

#  списки ненужных значений, которые надо будет удалить
list_highway_notok = ['steps', 'pedestrian', 'footway', 'path', 'raceway', 'road', 'track']
# list_other_tags_notok = ['power', 'natural', 'grass', 'wood', 'mud', 'sand', 'admin_level', 'aeroway']

# list_waterway = ['riverbank', 'river', 'stream', 'ditch', 'canal',
#        'waterbank', 'dam', 'drain']
# list_aerialway = ['drag_lift', 'chair_lift', 't-bar']
# list_barrier = ['fence', 'retaining_wall', 'wall', '*', 'guard_rail', 'kerb',
#        'gate', 'yes', 'bollard', 'lift_gate', 'block', 'handrail']
# list_man_made = ['pier', 'gasometer', 'storage_tank', 'pipeline',
#        'wastewater_plant', 'survey_point', 'tower', 'water_works',
#        'lighthouse', 'embankment']
#


# удаление кривых символов в строке (которые не преобразовались по unicode)

list_columns = [city_graph['name'], city_graph['other_tags']]
list_new_columns = []
for column in list_columns:
    list_strings = []
    for row in column:
        new_row = row
        if (isinstance(bytes(), type(row)) == True):
            list_new_values = []
            new_value = ""
            for j in range(len(row)):
                try:
                    new_value = row[j:j+1].decode() 
                    # декодирование по одному байтовому символу
                except (UnicodeDecodeError, AttributeError):
                    try:
                        new_value = row[j:j+2].decode() 
                        # у кириллицы на одну букву два байтовых символа
                    except (UnicodeDecodeError, AttributeError):
                        new_value = ""
                list_new_values.append(new_value)
            new_string = ""
            for i in list_new_values:
                new_string = new_string+i
            if (len(new_string.encode('utf-8')) >= 254): 
                # максимальная длина строки в shp - 255
                new_string = new_string[:254]
            new_row = new_string + '"' #
        list_strings.append(new_row)
#
    list_new_columns.append(list_strings)
# 
# len(list_new_columns[0])
city_graph['name'] = list_new_columns[0]
city_graph['other_tags'] = list_new_columns[1]
#


# удаление строк, содержаших ненужные значения
city_graph = city_graph[
    (city_graph.waterway.isna()) 
    # только пустые значения подходят == #(~city_graph.aerialway.isin(list_waterway))
                       & (city_graph.aerialway.isna()) 
                       & (city_graph.barrier.isna())
                       & (city_graph.man_made.isna())
                       & (~city_graph.highway.isin(list_highway_notok))
                       & (~city_graph['other_tags'].str.contains("natural", na=False))
                       & (~city_graph['other_tags'].str.contains("power", na=False))
                       & (~city_graph['other_tags'].str.contains("grass", na=False))
                       & (~city_graph['other_tags'].str.contains("wood", na=False))
#     & (~(city_graph['other_tags'].str.contains("surface", na=False) & (city_graph.name.isna())))
                       & (~city_graph['other_tags'].str.contains("mud", na=False))
                       & (~(city_graph['other_tags'].str.contains("sand", na=False) & city_graph.name.isna()))
    & (~city_graph['other_tags'].str.contains("admin_level", na=False))
    & (~city_graph['other_tags'].str.contains("aeroway", na=False))
    & (~city_graph['other_tags'].str.contains("piste", na=False))
    & (~city_graph['other_tags'].str.contains("building", na=False))
    & (~city_graph['other_tags'].str.contains("ferry", na=False))
    & (~city_graph['other_tags'].str.contains("land", na=False))
    & (~city_graph['other_tags'].str.contains("description", na=False))
    & (~city_graph['other_tags'].str.contains("private", na=False))
    & (~city_graph['other_tags'].str.contains("leaf_type", na=False))
    & (~city_graph['other_tags'].str.contains('attraction', na=False))
#     & (~city_graph['other_tags'].str.contains("unpaved", na=False))
    & (~((city_graph.z_order == 0) & (city_graph.name.isna())))
    & (~((city_graph.highway == 'service') & (city_graph.name.isna())))
                             ].reset_index(drop=True)
#print(len(city_graph))



# фильтр жд и трамвайных путей
rail_tram = city_graph[(city_graph['other_tags'].str.contains('=>"tram"', na=False))]
rail_main = city_graph[(city_graph['other_tags'].str.contains('"usage"=>"main"', na=False))]

city_graph = city_graph[
    (~city_graph['other_tags'].str.contains('=>"rail"', na=False))
    & (~city_graph['other_tags'].str.contains('railway', na=False))
                     ]
city_graph = city_graph.append(rail_tram)
city_graph = city_graph.append(rail_main)

city_graph = city_graph.reset_index(drop=True)

#print(len(city_graph))
print()
print("Граф обрабатывается, подождите...")
print()
# #Обработка графа - дробление ребер по перекресткам и создание узлов (nodes)


# здесь происходит дробление ребер по всем пересечениям (даже на многоуровневых эстакадах)
# обработка эстакад будет ниже
lines = list(city_graph.geometry)
graph = unary_union(lines)
res_graph = gpd.GeoDataFrame(graph) 
# если сделать через geometry=[graph] он делает из графа один большой multilinestring
res_graph = res_graph.rename(columns={0:'geometry'})
res_graph.crs='epsg:4326'
res_graph = res_graph.to_crs('epsg:4326')

#print(len(res_graph))
#res_graph.head(2)

# подтягивание полей с информацией по пересечению геометрий
graph_info = gpd.sjoin(res_graph, city_graph, how='left', 
                           op='within').drop("index_right", axis=1).reset_index(drop=True)
# print(len(res_graph))
#print(len(graph_info))
# graph_info.head(2)



# функция обрезки ребер, 
# чтобы при пересечении лишние не приклеивались (те, что на концах ребер)
def cut(line, distance):
    # Cuts a line in two at a distance from its starting point
    if distance <= 0.0 or distance >= line.length:
        return [LineString(line)]
    coords = list(line.coords)
    for i, p in enumerate(coords):
        pd = line.project(Point(p))
        if pd == distance:
            return [
                LineString(coords[:i+1]),
                LineString(coords[i:])]
        if pd > distance:
            cp = line.interpolate(distance)
            return [
                LineString(coords[:i] + [(cp.x, cp.y)]),
                LineString([(cp.x, cp.y)] + coords[i:])]
# 
# вызов функции два раза - чтобы ребро обрезать с двух сторон
def cut_piece(line, start, end):
    """ From a linestring, this cuts a piece of length lgth at distance.
    Needs cut(line,distance) func from above ;-) 
    """
    precut = cut(line, start)[0]
    result = cut(precut, end)[1]
#     try:
#         result = cut(precut, end)[1]
#     except:
#         result = precut
    return result
#



# вызов функции обрезки
# copy_nans_gdf = nans_gdf.copy()
copy_graph_info = graph_info.copy()
copy_nans_gdf = copy_graph_info[copy_graph_info.osm_id.isna()]

list_new_geo = []

list_geo_old = list(copy_nans_gdf.to_crs('epsg:32637').geometry) 
# преобразование - чтобы в метрах отсечь

for i in range(len(list_geo_old)):
    line = list_geo_old[i]
    length_line = line.length
    start = length_line - 0.05 # это длина в метрах - 0.05 метров
    end = length_line - start
    cutted_line = cut_piece(line, start, end)
    list_new_geo.append(cutted_line)
# 

copy_nans_gdf['new_geo'] = list_new_geo
copy_nans_gdf = copy_nans_gdf.rename(columns={'geometry':'old_geo', 'new_geo':'geometry'})
del copy_nans_gdf['old_geo']

copy_nans_gdf.crs = 'epsg:32637'
copy_nans_gdf = copy_nans_gdf.to_crs('epsg:4326')



#  здесь реализован поиск многоуровневых эстакад (ребер, и их id)
# nans_gdf = graph_info[graph_info.osm_id.isna()]

nans_gdf = copy_nans_gdf.copy()

nans_gdf_inter_1 = gpd.sjoin(nans_gdf[['geometry']], city_graph, how='left', 
                           op='intersects').drop("index_right", axis=1).reset_index(drop=True)

non_nan_inter = nans_gdf_inter_1[~nans_gdf_inter_1.osm_id.isna()]

nan_inter = nans_gdf_inter_1[nans_gdf_inter_1.osm_id.isna()]
nans_gdf_buffer = nan_inter.copy()
nans_gdf_buffer = nans_gdf_buffer.to_crs('epsg:32637')
nans_gdf_buffer['geometry'] = nans_gdf_buffer.geometry.buffer(0.05)
nans_gdf_buffer = nans_gdf_buffer.to_crs('epsg:4326')

nans_gdf_inter_2 = gpd.sjoin(nans_gdf_buffer[['geometry']], city_graph, how='left', 
                           op='intersects').drop("index_right", axis=1).reset_index(drop=True)

nans_gdf_inter = non_nan_inter.append(nans_gdf_inter_2)

list_nans = list(nans_gdf_inter[~nans_gdf_inter.osm_id.isna()].osm_id.unique())
#print(len(list_nans))



# разбиение ребер, которые пересекают эстакады
# но не все между собой, а только те, 
# которые не входят в эстакаду, потому что ее надо оставить целостной
list_nans = list(nans_gdf_inter[~nans_gdf_inter.osm_id.isna()].osm_id.unique())

for_one_nan = gpd.GeoDataFrame()


for osmid in range(len(list_nans)):
    osm_id_one = list_nans[osmid]
# osm_id_one = '45724863'
# one_nan = city_graph[city_graph.osm_id == list_nans[0]].copy()[['geometry']]
    one_nan = city_graph[city_graph.osm_id == osm_id_one].copy()[['geometry']]


    inter_one_nan = gpd.sjoin(one_nan, city_graph, how='left', 
                           op='intersects').drop("index_right", axis=1).reset_index(drop=True)
    list_inter_one_nan = list(inter_one_nan[~inter_one_nan.osm_id.isna()].osm_id.unique())
    # удалить все ребра, с которыми нельзя пересекать
    list_good_one = list(set(list_inter_one_nan) - set(list_nans)) 
    list_good_one.append(osm_id_one) #для unary_union нужно само ребро
    unary_one = list(city_graph[city_graph.osm_id.isin(list_good_one)].geometry)

    try:
        graph_one = unary_union(unary_one)
    except (AttributeError):
        #print("except")
        graph_one = unary_one
    # 
    graph_one_res = [graph_one]
    gdf_graph_one = gpd.GeoDataFrame(geometry=graph_one_res)
    # gdf_graph_one = gdf_graph_one.rename(columns={0:'geometry'})
    gdf_graph_one.crs='epsg:4326'
    gdf_graph_one = gdf_graph_one.to_crs('epsg:4326')



    # разделение мультилиний (разбитое ребро превращается в мультилинию) на отдельные линии
    def transf(x):
        mapp = mapping(x)
        if mapp['type'] == "MultiLineString":
            y = pd.DataFrame([[x] for x in mapp['coordinates']], columns=['geometry'])
        else:
            y = pd.DataFrame([[mapping(x)['coordinates']]],columns=['geometry'])
        return y
    # 

    ####################
    big_df = pd.DataFrame()
    # print(len(gdf_graph_one))
    for ix, row in gdf_graph_one.iterrows():
        big_df = big_df.append(transf(row['geometry']).assign(keys=ix)).reset_index(drop=True)
    #
    list_geo =[]
    for row in big_df.geometry:
        lines = LineString(row)
        list_geo.append(lines)
    big_df['new_geo'] = list_geo
    big_df = big_df.rename(columns={'geometry':'geometry_old', 'new_geo':'geometry'})



    gdf_graph_one_new = gpd.GeoDataFrame(big_df[['geometry']].copy().reset_index(drop=True))
    gdf_graph_one_new.crs='epsg:4326'

    graph_one_info = gpd.sjoin(gdf_graph_one_new, city_graph, how='left', 
                           op='within').drop("index_right", axis=1).reset_index(drop=True)
    # print(len(graph_one_info))
    graph_one_info = graph_one_info[graph_one_info.osm_id == osm_id_one].reset_index(drop=True)
    # print(len(graph_one_info))


    for_one_nan = for_one_nan.append(graph_one_info).reset_index(drop=True)
#
# print(len(for_one_nan))
# for_one_nan.head(2)



# объединение все ребер в одни граф, кроме пустых значений и дубликатов
list_for_one = list(for_one_nan[~for_one_nan.osm_id.isna()].osm_id.unique())
not_nan_graph = graph_info.copy()
not_nan_graph = not_nan_graph[(~not_nan_graph.osm_id.isna()) 
                              & (~not_nan_graph.osm_id.isin(list_for_one))
                             ]
graph_full = not_nan_graph.append(for_one_nan).reset_index(drop=True)
graph_full['z_order'] = graph_full['z_order'].astype(np.int64)


# создание графа networkx
G = momepy.gdf_to_nx(graph_full, approach='primal')
#print(nx.info(G))


# выбор наибольшего графа из подграфов 
# (когда ребра удаляются выше, остаются подвешенные куски графа, их надо удалить)
cur_graph = G # whatever graph you're working with

if not nx.is_connected(cur_graph):
    # get a list of unconnected networks
    def connected_component_subgraphs(cur_graph):
        for c in nx.connected_components(cur_graph):
            yield cur_graph.subgraph(c)
    sub_graphs = connected_component_subgraphs(cur_graph)
    list_graph = []
    for i in sub_graphs:
        list_graph.append(i)

    main_graph = list_graph[0]

    # find the largest network in that list
    for sg in list_graph:
        if len(sg.nodes()) > len(main_graph.nodes()):
            main_graph = sg

    cur_graph = main_graph
    #print(nx.info(cur_graph))
#


# формирование таблиц из графа и узлов (nodes)
nodes, new_graph = momepy.nx_to_gdf(cur_graph)

nodes.crs='epsg:4326'
nodes = nodes.to_crs('epsg:4326')

new_graph.crs='epsg:4326'
new_graph = new_graph.to_crs('epsg:4326')
#new_graph.head(2)


new_graph.to_file('./data/res/edges/new_graph_{}_{}.shp'.format(place, str_date), encoding='utf-8')

nodes.to_file('./data/res/nodes/nodes_{}_{}.shp'.format(place, str_date), encoding='utf-8')

print("Обработанный граф и узлы выгружены в папки: ./data/res/")