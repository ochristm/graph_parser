print("Running...")

import os
#from tqdm.notebook import tqdm

# # В случе ошибки RuntimeError: b'no arguments in initialization list'
# # Если действие выше не помогло, то нужно задать системной переменной PROJ_LIB
# # явный путь к окружению по аналогии ниже
# Для настройки проекции координат, поменять на свой вариант
import conda
conda_file_dir = conda.__file__
conda_dir = conda_file_dir.split('lib')[0]
proj_lib = os.path.join(conda_dir, 'Library\share')
# proj_lib = os.path.join(os.path.join(conda_dir, 'pkgs'), 'proj4-5.2.0-h6538335_1006\Library\share')
path_gdal = os.path.join(proj_lib, 'gdal')
os.environ ['PROJ_LIB']=proj_lib
os.environ ['GDAL_DATA']=path_gdal

# os.environ ['PROJ_LIB']=r'C:\Users\popova_kv\AppData\Local\Continuum\anaconda3\Library\share'
# os.environ ['GDAL_DATA']=r'C:\Users\popova_kv\AppData\Local\Continuum\anaconda3\Library\share\gdal'

import pandas as pd
import numpy as np
from datetime import datetime
import geopandas as gpd
import shapely
#отключить предупреждения pandas (так быстрее считает!!!):
pd.options.mode.chained_assignment = None

from shapely import wkt
from shapely.wkt import loads
from shapely.geometry import Point, LineString, mapping, shape
from shapely.ops import unary_union

import networkx as nx
import momepy
import re

import math
from math import degrees, acos
###########################

#filename = './data/map_2_Yaroslavl,Russia_20200430_2004.osm'
# ./data/raw/osm/map_2_Ярославль_20200521_0934.osm
#filename = parse_osm.filename
filename = ''

while (os.path.isfile(filename) != True):
    print("Введите путь и название нового графа, пример:")
    print(".\\data\\1701435\\20200608_1650\\res\\new_graph_2_Ярославль_20200608_1650.shp")
    filename = input()
    filename = '.\data' + filename.split(".\data")[1]
    if os.path.isfile(filename) == False:
        print("Файл не найден, попробуйте снова")
    # else:
        # print("Файл найден")
# 

try:
	list_split = filename[:-4].rsplit("_")
	str_date = list_split[-2] + "_" + list_split[-1]
	place = list_split[-3]
	buff_km = list_split[-4]
	poly_osmid = filename.split("data\\")[1].split("\\")[0]
except:
	print("Enter place name:")
	place = input()
	if len(place) == 0:
		place = 'city'
	str_date = "{:%Y%m%d_%H%M}".format(datetime.now())
	buff_km = 0
	poly_osmid = len(filename)
	print("place_id:",poly_osmid)
#

###########################
#path_new = os.getcwd()
path_data = '.\\data\\' + str(poly_osmid) + '\\' + str_date

#path_tuon = path_data = '\\turn_onew'

path_raw = path_data + '\\raw'
path_raw_csv = path_raw
#path_raw_shp = path_raw
path_raw_shp_layers = path_raw + '\\layers'

path_res = path_data + '\\res'
path_res_edges = path_res
path_res_nodes = path_res
path_tuon = path_res #path_data + '\\turn_onew'

# try:
    # os.mkdir(path_tuon)
    # # print ("Создана директория %s \n" % path_tuon)
# except OSError:
    # print ("Не удалось создать директорию: %s \n" % path_tuon)
    # print("Возможно, она уже создана")
# #

time_start = "{:%H:%M:%S}".format(datetime.now())
print("time start:", time_start)


###########################

new_graph = gpd.read_file(r'{}\\new_graph_{}_{}_{}.shp'.format(path_res_edges,buff_km, place, str_date), encoding = 'utf-8')
all_nodes = gpd.read_file(r'{}\\nodes_{}_{}_{}.shp'.format(path_res_nodes,buff_km, place, str_date), encoding = 'utf-8')

try:
    gdf_other_lines = gpd.read_file(r'{}\\other_lines_{}_{}_{}.shp'.format(path_raw_shp_layers,buff_km, place, str_date), encoding = 'utf-8')
    gdf_other_points = gpd.read_file(r'{}\\other_points_{}_{}_{}.shp'.format(path_raw_shp_layers,buff_km, place, str_date), encoding = 'utf-8')
except:
    lst_poi_col = ['osm_id_res','restr_type','role','osm_id','geo_type','geo_line','geometry']
    lst_ln_col = ['osm_id_res','restr_type','role','osm_id','geo_type','p_via','geometry']
    gdf_other_lines = gpd.GeoDataFrame(columns=lst_poi_col,data=None)
    gdf_other_points = gpd.GeoDataFrame(columns=lst_ln_col,data=None)
#
#Specify dtype to eliminate this error
#DtypeWarning: Columns (4) have mixed types.Specify dtype option on import or set low_memory=False.
dtype={'osm_id':np.int64, 'name':str, 'highway':str, 'waterway':str, 
       'aerialway':str, 'barrier':str,'man_made':str, 'z_order':np.int64, 'other_tags':str}
csv_ot = pd.read_csv(r'{}\\csv_{}_{}_{}.csv'.format(path_raw_csv,buff_km, place, str_date), dtype=dtype, encoding = 'utf-8', sep=';')

######
len_elem = len(new_graph)
time_min = int((len_elem / 210) / 60) 
time_max = int((len_elem / 88) / 60) 
print("Estimated time: {} to {} minutes".format(time_min,time_max))
print("Please wait...")

##########################


all_nodes = all_nodes[['NO', 'geometry']]
all_nodes = all_nodes.rename(columns={'NO':'nodeID'})
all_nodes_copy = all_nodes.copy()
all_nodes_copy['str_geo'] = all_nodes_copy['geometry'].astype(str)

#####################
# lst_tss = []
# i=0
# for i in range(len(new_graph)):
    # if new_graph.TSYSSET[i] == 'T':
        # lst_tss.append('TM')
    # else:
        # lst_tss.append(new_graph.TSYSSET[i])
# # 
# new_graph['TSYSSET']=lst_tss
#####################

# подтянуть обрезавшиеся теги (shp обрезает до 255 символов)
dict_ot = {}

i=0
for i in (range(len(csv_ot))):
    len_str = len(str(csv_ot.other_tags[i]))
    if len_str > 254:
        osmid = str(csv_ot.osm_id[i])
        dict_ot[osmid] = str(csv_ot.other_tags[i])
# 
new_tags = pd.DataFrame(columns=['osm_id', 'other_tags'])
new_tags['osm_id'] = list(dict_ot.keys())
new_tags['other_tags'] = list(dict_ot.values())

# ##################################
i=0
j=0
for i in (range(len(new_tags))):
    for j in new_graph[new_graph.osm_id.isin(new_tags.osm_id)].osm_id:
        ind = list(new_graph.osm_id).index(j)
        if new_graph.osm_id[ind] == new_tags.osm_id[i]:
            new_graph.other_tags[ind] = new_tags.other_tags[i]
# 


###############other_rel
def oneRestr(gdf_other_lines, gdf_other_points):

    gdf_other_lines = gdf_other_lines.rename(columns={'osm_id':'osm_id_graph',
                                                      'osm_id_res':'osm_id', 'restr_type':'other_tags'})
    gdf_other_points = gdf_other_points.rename(columns={'osm_id':'osm_id_graph',
                                                        'osm_id_res':'osm_id', 'restr_type':'other_tags'})
    new_o_lines = gdf_other_lines.copy()

    olines_first = new_o_lines[((new_o_lines.role == 'from') | (new_o_lines.role == '1from'))].reset_index(drop=True)
    olines_last = new_o_lines[((new_o_lines.role == 'to') | (new_o_lines.role == '1to'))].reset_index(drop=True)

    restr_points = gdf_other_points[['osm_id', 'other_tags', 'geometry']].copy()
    restr_points = restr_points.rename(columns={'geometry':'p_via'})

    restr_points = restr_points.merge(olines_first[['osm_id', 
                                                       'geometry']], how='left', on=['osm_id'])
    restr_points = restr_points.rename(columns={'geometry':'one_geometry'})


    restr_points = restr_points.merge(olines_last[['osm_id', 
                                                       'geometry']], how='left', on=['osm_id'])
    restr_points = restr_points.rename(columns={'geometry':'two_geometry'})

    lst_nan_restr = list(restr_points[restr_points.two_geometry.isna() | restr_points.one_geometry.isna()].osm_id.unique())
    restr_points = restr_points[~restr_points.osm_id.isin(lst_nan_restr)].reset_index(drop=True)

    def create_points(find_point):
        list_p_from = []
        list_p_to = []
        i=0
        for i in range(len(find_point)):
            if find_point.one_geometry[i].coords[0] == find_point.p_via[i].coords[0]:
                list_p_from.append(Point(find_point.one_geometry[i].coords[-1]))
            else:
                list_p_from.append(Point(find_point.one_geometry[i].coords[0]))
        # 
        i=0
        for i in range(len(find_point)):
            if find_point.two_geometry[i].coords[0] == find_point.p_via[i].coords[0]:
                list_p_to.append(Point(find_point.two_geometry[i].coords[-1]))
            else:
                list_p_to.append(Point(find_point.two_geometry[i].coords[0]))
        # 
        try:
            find_point['p_from'] = list_p_from
            find_point['p_to'] = list_p_to
        except:
            print("Error!")
        return find_point
    # 

    find_point = restr_points.copy()
    find_point = create_points(find_point)

    

    def PointBuffNode(gdf, column):
        gdf_copy = gdf.copy()
        gdf_copy['geometry'] = gdf_copy[column]
        gdf_w_buff = gpd.GeoDataFrame(gdf_copy)

        gdf_w_buff.crs='epsg:4326' 
        gdf_w_buff = gdf_w_buff.to_crs('epsg:32637')
        gdf_w_buff.geometry = gdf_w_buff.geometry.buffer(0.5)
        gdf_w_buff = gdf_w_buff.to_crs('epsg:4326')
        new_name = "buff_" + column
        gdf_w_buff[new_name] = gdf_w_buff.geometry
        
        inter_gdf_new = gpd.sjoin(gdf_w_buff, all_nodes, how='inner', 
                                  op='intersects').drop("index_right", axis=1).reset_index(drop=True)
        node_name = "node_" + column
        inter_gdf_new[node_name] = inter_gdf_new['nodeID']
        del inter_gdf_new['geometry'], inter_gdf_new['nodeID'], inter_gdf_new[new_name]
        return inter_gdf_new
    #
    find_point = find_point[['osm_id', 'other_tags', 'p_from', 'p_via', 
                                   'p_to']]
    gdf_new = PointBuffNode(find_point, 'p_from')
    gdf_new = PointBuffNode(gdf_new, 'p_via')
    gdf_new = PointBuffNode(gdf_new, 'p_to')
    gdf_new = gdf_new.rename(columns={'node_p_from':'node_from', 'node_p_to':'node_to', 
                                      'node_p_via':'node_via'})
    found_geo_points = gdf_new.copy()

    first_find = found_geo_points[
        (~(found_geo_points.node_from.isna())
        & ~(found_geo_points.node_via.isna())
        & ~(found_geo_points.node_to.isna()))].reset_index(drop=True)
    first_find['node_from'] = first_find['node_from'].astype(np.int64)
    first_find['node_via'] = first_find['node_via'].astype(np.int64)
    first_find['node_to'] = first_find['node_to'].astype(np.int64)


    nodes_restr = first_find[['osm_id', 'other_tags', 'node_from', 'node_via', 'node_to']].copy()
    nodes_restr = nodes_restr.drop_duplicates(nodes_restr.columns).reset_index(drop=True)
    
    return nodes_restr

############# END of Checking restrictions #############

try:
    nodes_restr = oneRestr(gdf_other_lines,gdf_other_points)
except:
    lst_nr = ['osm_id', 'other_tags', 'node_from', 'node_via', 'node_to']
    nodes_restr = gpd.GeoDataFrame(columns=lst_nr, data=None)

############# Find neighbours of graph #########

all_graph = momepy.gdf_to_nx(new_graph, approach='primal')

# поиск всех ближайших (только в первом колене) соседей для каждого узла
all_neigh = []
i=0
for i in (range(len(all_graph))):
    lst_neigh = []
    one_node = list(all_graph.nodes())[i]
    lst_neigh = list(nx.all_neighbors(all_graph, one_node))
#     lst_neigh.append(neighs)
    all_neigh.append(lst_neigh)
# 

# подтягивание node_id (потому что в поиске нашлась только их геометрия)
neigh_names = []
node_names = []
i=0
j=0
for i in (range(len(all_neigh))):
    neighs_one = all_neigh[i]
    names_one = []
    for j in range(len(neighs_one)):
        one_point = str(Point(neighs_one[j]))
        one_name = list(all_nodes_copy[all_nodes_copy.str_geo == one_point].nodeID)[0]
        names_one.append(one_name)
    neigh_names.append(names_one)
# 


# счет кол-ва соседей для каждого узла
df_neighs = all_nodes.sort_values(by=['nodeID'])[['nodeID']]
df_neighs['neighbours'] = neigh_names

list_cnt = []
i=0
for i in (range(len(df_neighs))):
    list_cnt.append(len(df_neighs.neighbours[i]))
df_neighs['cnt_neig'] = list_cnt
# df_neighs.head(3)

# разделение на отдельные from_to (изначально from - один, а to - список)
list_from = []
list_to = []
i=0
j=0
for i in (range(len(df_neighs))):
    for j in range(len(df_neighs.neighbours[i])):
        list_from.append(df_neighs.nodeID[i])
        list_to.append(df_neighs.neighbours[i][j])
# 


# формирование таблицы from_to и удаление односторонних пар в неправильном навправлении
df_from_to = new_graph[new_graph.NUMLANES != 0][['FROMNODENO', 'TONODENO', 'TSYSSET']]
df_from_to = df_from_to.drop_duplicates(df_from_to.columns).reset_index(drop=True)
df_from_to = df_from_to.rename(columns={'FROMNODENO':'node_from', 'TONODENO':'node_to'})
df_from_to['from_to'] = df_from_to['node_from'].astype(str) + "_" + df_from_to['node_to'].astype(str)


# for u_turn only (delete only from_via_to, from_to - is ok)
# to delete u_turns for railway - its impossible

rail_all = new_graph[new_graph.TSYSSET.isin(['E', 'TM', 'MTR'])].reset_index(drop=True)
rail_false = rail_all[['FROMNODENO', 'TONODENO']].copy()
rail_false['from_via_to'] = rail_false['FROMNODENO'].astype(str) \
+ "_" + rail_false['TONODENO'].astype(str) +  "_" + rail_false['FROMNODENO'].astype(str)

# if there are 2 neighbours - no u_turn, 
# because it should be allowed only on crossroads (3 and more neighbours)
no_u_turn = df_neighs[df_neighs.cnt_neig == 2].reset_index(drop=True)

df_from_via_to = df_from_to[['node_from', 'node_to', 'TSYSSET']].copy()
df_from_via_to = df_from_via_to.rename(columns={'node_from':'node_from_1', 
                                                'node_to':'node_from', 
                                                'TSYSSET':'TSYSSET_1'})
#
df_from_via_to = df_from_via_to.merge(df_from_to[['node_from', 'node_to', 'TSYSSET']], how='left', 
                                      on=['node_from']).reset_index(drop=True)
#
df_from_via_to = df_from_via_to.rename(columns={'node_from':'node_via', 
                                                'node_from_1':'node_from', 
                                                'TSYSSET':'TSYSSET_2'})
#
#########################

df_from_via_to = df_from_via_to.reset_index(drop=True)

lst_ok_type = []
i=0
for i in range(len(df_from_via_to)):
    if str(df_from_via_to.TSYSSET_1[i]) in str(df_from_via_to.TSYSSET_2[i]):
        lst_ok_type.append(df_from_via_to.TSYSSET_1[i])
    elif str(df_from_via_to.TSYSSET_2[i]) in str(df_from_via_to.TSYSSET_1[i]):
        lst_ok_type.append(df_from_via_to.TSYSSET_2[i])
    else:
        lst_ok_type.append("Wrong")
# 


df_from_via_to['TSYSSET'] = lst_ok_type

df_from_via_to = df_from_via_to[(df_from_via_to.TSYSSET != 'Wrong')].reset_index(drop=True)

# nans - only in node_to, they are ends, its ok to delete
df_from_via_to['node_to'] = df_from_via_to['node_to'].astype(np.int64)


df_from_via_to['from_via_to'] = df_from_via_to['node_from'].astype(str) \
+ "_" + df_from_via_to['node_via'].astype(str) +  "_" + df_from_via_to['node_to'].astype(str)

# delete u_turns for railway - its impossible
df_from_via_to = df_from_via_to[~df_from_via_to.from_via_to.isin(rail_false.from_via_to)]

# delete u_turns in the middle of the road
df_from_via_to = df_from_via_to[~((df_from_via_to.node_via.isin(no_u_turn.nodeID))
                                  & (df_from_via_to.node_from == df_from_via_to.node_to))]
#
df_from_via_to = df_from_via_to.reset_index(drop=True)


###################################
# only - delete other (node_from - node_via), except itself
# no - delete (node_from - node_via - node_to)
# no_entry - delete (node_from - node_via - node_to)
# no_exit - delete (node_from - node_via - node_to)
# except - create other table

# exceptions in restrictions, means those types of transport can drive that way
# will be prosessed later

#



####################################################################################



#
def twoRestr(nodes_restr):
    nr_copy = nodes_restr.copy()
    nr_copy['from_via'] = nr_copy['node_from'].astype(str) + "_" \
    + nr_copy['node_via'].astype(str) + "_"
    nr_copy['from_via_to'] = nr_copy['node_from'].astype(str) + "_" \
    + nr_copy['node_via'].astype(str) + "_" + nr_copy['node_to'].astype(str)

    # type_no - should be deleted exactly those paths
    restr_type_no = nr_copy[nr_copy.other_tags.str.contains("no_",
                                                            na=False)].reset_index(drop=True)
    # type_only - should be deleted other than those paths with same (node_from - node_via)
    restr_type_only = nr_copy[nr_copy.other_tags.str.contains("only",
                                                              na=False)].reset_index(drop=True)
    #
    list_dlt_only = []
    i=0
    for i in (range(len(restr_type_only))):
        good_fvt = restr_type_only.from_via_to[i]
        fv_group = restr_type_only.from_via[i]
        list_all = list(df_from_via_to[df_from_via_to.from_via_to.str.contains(fv_group, 
                                                                               na=False)].from_via_to)
        list_dlt = list_all[:]
        try:
            list_dlt.remove(good_fvt)
            if len(list_dlt) > 0:
                list_dlt_only.append(list_dlt)
        except:
            pass
    # 
    # создание объединенного (одного) списка из списка списков
    dlt_only = sum(list_dlt_only, [])
    
    return dlt_only,nr_copy,restr_type_no,restr_type_only
#

try:
    dlt_only,nr_copy,restr_type_no,restr_type_only = twoRestr(nodes_restr)
except:
    dlt_only = []
    lst_nr = ['osm_id', 'other_tags', 'node_from', 'node_via', 'node_to']
    nr_copy = gpd.GeoDataFrame(columns=lst_nr, data=None)
    restr_type_no = gpd.GeoDataFrame(columns=lst_nr, data=None)
    restr_type_only = gpd.GeoDataFrame(columns=lst_nr, data=None)

# exceptions
# исключения. это значит, что запрет действует для всех видов транспорта, кроме указанного в except
# значит, что указанный в except вид транспорта может совершать этот маневр

#####################################
###################################

etrain_fvt = df_from_via_to[((df_from_via_to.TSYSSET_1 == 'E')
                & (df_from_via_to.TSYSSET_2 == 'E'))].reset_index(drop=True)
etrain_fvt['TSYSSET'] = 'E'
#
tram_fvt = df_from_via_to[((df_from_via_to.TSYSSET_1.str.contains('TM'))
                & (df_from_via_to.TSYSSET_2.str.contains('TM')))].reset_index(drop=True)
tram_fvt['TSYSSET'] = 'TM'
#
metro_fvt = df_from_via_to[((df_from_via_to.TSYSSET_1.str.contains('MTR'))
                & (df_from_via_to.TSYSSET_2.str.contains('MTR')))].reset_index(drop=True)
metro_fvt['TSYSSET'] = 'MTR'
#
psv_fvt = df_from_via_to[((df_from_via_to.TSYSSET_1 == 'BUS,TB,MT')
                & (df_from_via_to.TSYSSET_2 == 'BUS,TB,MT'))].reset_index(drop=True)
psv_fvt['TSYSSET'] = 'BUS,TB,MT'
#
veh_all_fvt = df_from_via_to[((df_from_via_to.TSYSSET_1.str.contains('CAR'))
                & (df_from_via_to.TSYSSET_2.str.contains('CAR')))].reset_index(drop=True)
# delete psv_only paths from all_vehicle (one or two nodes are ok, but three - no)
veh_all_fvt = veh_all_fvt[~veh_all_fvt.from_via_to.isin(psv_fvt.from_via_to)]
veh_all_fvt['TSYSSET'] = 'CAR,BUS,TB,MT'
#
# delete restrictions "only"
veh_all_fvt = veh_all_fvt[~veh_all_fvt.from_via_to.isin(dlt_only)]
# delete restrictions "no"
veh_all_fvt = veh_all_fvt[~veh_all_fvt.from_via_to.isin(restr_type_no.from_via_to)]
veh_all_fvt = veh_all_fvt.reset_index(drop=True)
veh_all_fvt['TSYSSET'] = 'CAR,BUS,TB,MT'

# add - after deletion in all_veh, because some except_restrictions must be in all_veh
# psv_fvt = psv_fvt.append(except_psv).drop_duplicates().reset_index(drop=True)

all_fvt = veh_all_fvt.append(psv_fvt).append(tram_fvt).append(metro_fvt).append(etrain_fvt).reset_index(drop=True)

all_fvt = all_fvt.rename(columns={'node_from':'FROMNODENO', 'node_via':'VIANODENO', 'node_to':'TONODENO'})
del all_fvt['from_via_to'], all_fvt['TSYSSET_1'], all_fvt['TSYSSET_2']

nodes = all_nodes.copy()
my_turns = all_fvt.copy()

############################
    


#Find type of the turn
############################

copy_nodes = nodes.copy()
copy_nodes = copy_nodes.rename(columns={'nodeID':'NO'})
# copy_nodes = copy_nodes.to_crs('epsg:4326')

geo_turns = my_turns.copy()
fr_mrg = new_graph[new_graph.NUMLANES != 0][['FROMNODENO', 'TONODENO', 'geometry']]

geo_turns['real_FROMNODENO'] = geo_turns['FROMNODENO']
geo_turns['real_TONODENO'] = geo_turns['TONODENO']

geo_turns['TONODENO'] = geo_turns['VIANODENO']
geo_turns = geo_turns.merge(fr_mrg, how='left', 
                            on=['FROMNODENO', 'TONODENO'])
geo_turns = geo_turns.rename(columns={'geometry':'line_geo_from'})


geo_turns['TONODENO'] = geo_turns['real_TONODENO']
geo_turns['FROMNODENO'] = geo_turns['VIANODENO']
geo_turns = geo_turns.merge(fr_mrg, how='left', 
                            on=['FROMNODENO', 'TONODENO'])
geo_turns = geo_turns.rename(columns={'geometry':'line_geo_to'})

geo_turns['FROMNODENO'] = geo_turns['real_FROMNODENO']
geo_turns['TONODENO'] = geo_turns['real_TONODENO']

del geo_turns['real_FROMNODENO']
del geo_turns['real_TONODENO']
geo_turns = geo_turns[((~geo_turns.line_geo_from.isna()) & (~geo_turns.line_geo_to.isna()))].reset_index(drop=True)

############ Function to find turn between points

def calcDir(p0, p1, p2):
    v1x = float(p1[0]) - float(p0[0])
    v1y = float(p1[1]) - float(p0[1])
    v2x = float(p2[0]) - float(p1[0])
    v2y = float(p2[1]) - float(p1[1])
    if v1x * v2y - v1y * v2x > 0.0:
        return 'Left'
    elif v1x * v2y - v1y * v2x < 0.0:
        return 'Right'
    else:
        return 'Straight'
# 

################ Find start, via, end points
################ Find turn between three points

lst_p_from = []
lst_p_via = []
lst_p_to = []
lst_turns = []
i=0

for i in (range(len(geo_turns))):
    p_via = list(nodes.geometry[nodes.nodeID == geo_turns.VIANODENO[i]])[0]

    if geo_turns.line_geo_from[i].coords[-1] == p_via.coords[0]:
        p_from = Point(geo_turns.line_geo_from[i].coords[-2])
    else:
        p_from = Point(geo_turns.line_geo_from[i].coords[1])
    # 
    if geo_turns.line_geo_to[i].coords[0] == p_via.coords[0]:
        p_to = Point(geo_turns.line_geo_to[i].coords[1])
    else:
        p_to = Point(geo_turns.line_geo_to[i].coords[-2])
    #
    turn_to = calcDir(p_from.coords[0], p_via.coords[0], p_to.coords[0])
    lst_turns.append(turn_to)
    lst_p_from.append(p_from)
    lst_p_via.append(p_via)
    lst_p_to.append(p_to)
# 

try:
    geo_turns['geo_from'] = lst_p_from
    geo_turns['geo_via'] = lst_p_via
    geo_turns['geo_to'] = lst_p_to
    geo_turns['turn_to'] = lst_turns
except:
    print("Error_p_from_geo")


################## Find inner_angle
lst_angle = []
lst_notok = []
lst_ok = []
i = 0
for i in (range(len(geo_turns))):
    line_from = LineString([geo_turns.geo_from[i], geo_turns.geo_via[i]])
    line_to = LineString([geo_turns.geo_via[i], geo_turns.geo_to[i]])
    line_via = LineString([geo_turns.geo_from[i], geo_turns.geo_to[i]])
    A = line_from.length #значения слишком маленькие  * 10000
    B = line_to.length
    C = line_via.length
    try:
        angle = math.degrees(math.acos((A * A + B * B - C * C)/(2.0 * A * B)))
    except:
#         if (A + B) <= C:
        try:
            new_val = ((C - (A + B)) + 0.000002) / 2 # if (A + B) <= C - its an error, add a little to both
            A = A + new_val
            B = B + new_val
            angle = math.degrees(math.acos((A * A + B * B - C * C)/(2.0 * A * B)))
            lst_ok.append(i)
        except:
            angle = math.degrees(math.atan2(A,B))
            lst_notok.append(i)
    angle = int(round(angle, 0))
    lst_angle.append(angle)
# 

try:
    geo_turns['inner_angle'] = lst_angle
except:
    print("Errorlst_angle")

#

my_turns_w_graph = geo_turns.copy()

my_turns_w_graph['real_FROMNODENO'] = my_turns_w_graph['FROMNODENO']
my_turns_w_graph['real_TONODENO'] = my_turns_w_graph['TONODENO']

for_merge = new_graph[new_graph.NUMLANES != 0][['osm_id', 'name', 'highway', 'z_order',
                                                     'FROMNODENO', 'TONODENO']].copy()

my_turns_w_graph['TONODENO'] = my_turns_w_graph['VIANODENO']
my_turns_w_graph = my_turns_w_graph.merge(for_merge, how='left', on=['FROMNODENO', 'TONODENO'])


my_turns_w_graph['FROMNODENO'] = my_turns_w_graph['VIANODENO']
my_turns_w_graph['TONODENO'] = my_turns_w_graph['real_TONODENO']

my_turns_w_graph = my_turns_w_graph.merge(for_merge, how='left', on=['FROMNODENO', 'TONODENO'])


my_turns_w_graph['FROMNODENO'] = my_turns_w_graph['real_FROMNODENO']
del my_turns_w_graph['real_FROMNODENO']
del my_turns_w_graph['real_TONODENO']

node_geo_angle_graph = my_turns_w_graph.copy()

node_geo_angle_graph['from_via_type'] = node_geo_angle_graph.FROMNODENO.astype(str) + "_" +\
+ node_geo_angle_graph.VIANODENO.astype(str) + "_" +\
+ node_geo_angle_graph.TSYSSET.astype(str)

dct_gr_ang = {}
i=0
for i in (range(len(node_geo_angle_graph))):
    one_group = node_geo_angle_graph.from_via_type[i]
    if one_group not in dct_gr_ang.keys():
        lst_gr_ang = []
        dct_gr_ang[one_group] = lst_gr_ang
    dct_gr_ang[one_group] = dct_gr_ang[one_group] + [node_geo_angle_graph.inner_angle[i]]
# 

df_dict = pd.DataFrame(columns=['from_via_type', 'group_angles'])
df_dict['from_via_type'] = dct_gr_ang.keys()
df_dict['group_angles'] = dct_gr_ang.values()


node_geo_angle_graph = node_geo_angle_graph.merge(df_dict, how='left', on=['from_via_type'])

node_geo_angle_graph = node_geo_angle_graph.drop_duplicates(['FROMNODENO', 'VIANODENO', 'TONODENO', 
                                                             'TSYSSET', 'osm_id_x', 'name_x',
                                                             'highway_x', 'z_order_x', 'osm_id_y', 
                                                             'name_y', 'highway_y','z_order_y', 
                                                             'turn_to', 'inner_angle', 'from_via_type'])
node_geo_angle_graph = node_geo_angle_graph.reset_index(drop=True)
#
############# Check angle type
lst_typeno = []
i=0
for i in (range(len(node_geo_angle_graph))):
    if node_geo_angle_graph.FROMNODENO[i] == node_geo_angle_graph.TONODENO[i]:
        lst_typeno.append(4) #u_turn
    else:
        if node_geo_angle_graph.osm_id_x[i] == node_geo_angle_graph.osm_id_y[i]:
            lst_typeno.append(2) #straight_on
        else:
            if ((node_geo_angle_graph.name_x[i] == node_geo_angle_graph.name_y[i]) 
                & (node_geo_angle_graph.name_x[i] != None)):
                lst_typeno.append(2) #straight_on
            else:
                if len(node_geo_angle_graph.group_angles[i]) == 1:
                    lst_typeno.append(2) #straight_on
                else:
                    if ((node_geo_angle_graph.inner_angle[i] < 30) & (node_geo_angle_graph.turn_to[i] =='Left')):
                        row_len = len(node_geo_angle_graph[((node_geo_angle_graph.FROMNODENO == node_geo_angle_graph.TONODENO[i]) 
                                                            & (node_geo_angle_graph.VIANODENO == node_geo_angle_graph.VIANODENO[i]) 
                                                            & (node_geo_angle_graph.TONODENO == node_geo_angle_graph.FROMNODENO[i]))])
                        if row_len == 0:
                            lst_typeno.append(5) #if reverse turn dont exist, delete left later
                        else:
                            lst_typeno.append(3) #left_turn
                    else:
                        if ((node_geo_angle_graph.inner_angle[i] > 150) | (node_geo_angle_graph.inner_angle[i] < 30)):
                            lst_typeno.append(2) #straight_on
                        else:
                            if node_geo_angle_graph.turn_to[i] == 'Left':
                                lst_typeno.append(3) #left_turn
                            elif node_geo_angle_graph.turn_to[i] == 'Right':
                                lst_typeno.append(1) #right_turn
                            else:
                                lst_typeno.append(2) #straight_on
# # 
# node_geo_angle_graph = node_geo_angle_graph.drop_duplicates(node_geo_angle_graph.columns)
copy_ngag = node_geo_angle_graph.copy()
copy_ngag['TYPENO'] = lst_typeno
copy_ngag = copy_ngag[['FROMNODENO', 'VIANODENO', 'TONODENO', 'TSYSSET', 'TYPENO']]

zero_car = nodes_restr.copy()
zero_car = zero_car[zero_car.other_tags.str.contains("no_", na=False)].reset_index(drop=True)
lst_zc = []
i=0
for i in range(len(zero_car)):
    if "psv" in zero_car.other_tags[i]:
        lst_zc.append('CAR')
    else:
        lst_zc.append('CAR,BUS,TB,MT')
# 
zero_car['TSYSSET'] = lst_zc
zero_car['TYPENO'] = 0
zero_car = zero_car.rename(columns={'node_from':'FROMNODENO',
                                    'node_via':'VIANODENO', 'node_to':'TONODENO', 
                                    'osm_id':'OSM_RELATION_ID'})
#
zero_car = zero_car[['FROMNODENO', 'VIANODENO', 'TONODENO', 'TSYSSET', 'TYPENO', 'OSM_RELATION_ID']]

zero_car_2 = df_from_via_to[((df_from_via_to.from_via_to.isin(dlt_only)) 
                & (df_from_via_to.TSYSSET_1 == df_from_via_to.TSYSSET_2))].reset_index(drop=True)
#
try:
    zero_car_2 = zero_car_2.merge(nodes_restr[['osm_id', 'other_tags', 'node_from', 'node_via']], 
                                  how='left', on=['node_from', 'node_via'])
    #
    zero_car_2 = zero_car_2[~zero_car_2.osm_id.isna()]

    zero_car_2 = zero_car_2[zero_car_2.other_tags.str.contains("only_", na=False)]
    #
except:
    zero_car_2['osm_id'],zero_car_2['other_tags'],zero_car_2['node_from'],zero_car_2['node_via'] = None,None,None,None
    pass
#

zero_car_2 = zero_car_2.rename(columns={'node_from':'FROMNODENO',
                                    'node_via':'VIANODENO', 'node_to':'TONODENO', 
                                    'osm_id':'OSM_RELATION_ID'})
zero_car_2['TSYSSET'] = 'CAR'
zero_car_2['TYPENO'] = 0
cp_zc_2 = zero_car_2.copy()
zero_car_2 = zero_car_2[['FROMNODENO', 'VIANODENO', 'TONODENO', 'TSYSSET', 'TYPENO', 'OSM_RELATION_ID']]


all_zero_car = zero_car.append(zero_car_2).reset_index(drop=True)
all_zero_car = all_zero_car.drop_duplicates(all_zero_car.columns)

#

try:
    except_restr = nr_copy[nr_copy.other_tags.str.contains("except",
                                                           na=False)].reset_index(drop=True)
    except_psv = except_restr[except_restr.other_tags.str.contains("psv", na=False)].reset_index(drop=True)
    except_psv['TSYSSET'] = 'BUS,TB,MT'

    except_psv = except_psv.rename(columns={'node_from':'FROMNODENO',
                                        'node_via':'VIANODENO', 'node_to':'TONODENO', 
                                        'osm_id':'OSM_RELATION_ID'})
    #
except:
    lst_nr = ['osm_id', 'other_tags', 'node_from', 'node_via', 'node_to']
    except_psv = gpd.GeoDataFrame(columns=lst_nr, data=None)
#

# 
lst_type_psv = []
i=0
for i in range(len(except_psv)):
    if "left" in except_psv.other_tags[i]:
        lst_type_psv.append(3)
    elif "right" in except_psv.other_tags[i]:
        lst_type_psv.append(1)
    elif "u_turn" in except_psv.other_tags[i]:
        lst_type_psv.append(4)
    else:
        lst_type_psv.append(2)
# 
try:
    except_psv['TYPENO'] = lst_type_psv
except:
    print("Error_psv")
# 
except_psv = except_psv[['FROMNODENO', 'VIANODENO', 'TONODENO', 'TSYSSET', 'TYPENO', 'OSM_RELATION_ID']]


####################################
# подтягивание osm_id 
tmp_copy = copy_ngag.copy()
tmp_copy['from_via_to'] = tmp_copy['FROMNODENO'].astype(str) \
+ "_" + tmp_copy['VIANODENO'].astype(str) +  "_" + tmp_copy['TONODENO'].astype(str)

no_osm = tmp_copy[~tmp_copy.from_via_to.isin(nr_copy.from_via_to)].reset_index(drop=True)
no_osm['OSM_RELATION_ID'] = None
del no_osm['from_via_to']

tmp_restr = tmp_copy[tmp_copy.from_via_to.isin(nr_copy.from_via_to)].reset_index(drop=True)

try:
    tmp_restr = tmp_restr.merge(nr_copy[['osm_id', 
                                         'other_tags', 'from_via_to']], on=['from_via_to'])
except:
    tmp_restr['osm_id'],tmp_restr['other_tags'],tmp_restr['from_via_to'] = None,None,None
#

lst_restr_type = []
i=0
for i in range(len(tmp_restr)):
    if "left" in tmp_restr.other_tags[i]:
        lst_restr_type.append(3)
    elif "right" in tmp_restr.other_tags[i]:
        lst_restr_type.append(1)
    elif "u_turn" in tmp_restr.other_tags[i]:
        lst_restr_type.append(4)
    else:
        lst_restr_type.append(2)
# 

tmp_restr['TYPENO'] = lst_restr_type
tmp_restr = tmp_restr.rename(columns={'osm_id':'OSM_RELATION_ID'})
del tmp_restr['from_via_to'], tmp_restr['other_tags']

total_ngag = no_osm[['FROMNODENO', 'VIANODENO', 'TONODENO', 'TSYSSET', 'TYPENO',
       'OSM_RELATION_ID']].append(tmp_restr[['FROMNODENO', 'VIANODENO', 'TONODENO', 'TSYSSET', 'TYPENO',
       'OSM_RELATION_ID']]).reset_index(drop=True)
#
    
final_fvt = total_ngag.copy()
# final_fvt['OSM_RELATION_ID'] = None
final_fvt = final_fvt[final_fvt.TYPENO != 5]
final_fvt = final_fvt.append(except_psv).append(all_zero_car).reset_index(drop=True)

final_fvt = final_fvt.drop_duplicates(final_fvt.columns).reset_index(drop=True)
####################################

final_fvt.to_csv("{}\\final_fvt_{}_{}_{}.csv".format(path_tuon, buff_km, place, str_date), sep=";", encoding="utf-8", index=False)

time_end = "{:%H:%M:%S}".format(datetime.now())
print("time end:", time_end)
print("Results are in folder 'res'")
print("Done")

