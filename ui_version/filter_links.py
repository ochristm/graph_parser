#############################
import gdf_from_osm
#############################

print()
print("___________________________")
print("Part 3. Processing a graph (filtering and creating nodes)")
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

import pandas as pd
import numpy as np
from datetime import datetime
import geopandas as gpd
import shapely

from tqdm.notebook import tqdm

# # В случе ошибки RuntimeError: b'no arguments in initialization list'
# # Если действие выше не помогло, то нужно задать системной переменной PROJ_LIB
# # явный путь к окружению по аналогии ниже
# Для настройки проекции координат, поменять на свой вариант


# os.environ ['PROJ_LIB']=r'C:\Users\popova_kv\AppData\Local\Continuum\anaconda3\Library\share'
# os.environ ['GDAL_DATA']=r'C:\Users\popova_kv\AppData\Local\Continuum\anaconda3\Library\share\gdal'

#отключить предупреждения pandas (так быстрее считает!!!):
pd.options.mode.chained_assignment = None

from shapely import wkt
from shapely.wkt import loads
from shapely.geometry import Point, LineString, mapping, shape
from shapely.ops import unary_union

import networkx as nx
import momepy
import osmnx as ox
import re
import math

#######################

gdf_lines = gdf_from_osm.gdf_lines
gdf_multilines = gdf_from_osm.gdf_multilines
str_date = gdf_from_osm.str_date
place = gdf_from_osm.place
buff_km = gdf_from_osm.buff_km
poly_osmid = gdf_from_osm.poly_osmid
# BORDERS!!!!!
gdf_poly = gdf_from_osm.gdf_poly
# gdf_poly = ox.gdf_from_place(place, which_result=2, buffer_dist=int(buff_km)*1000)
# gdf_poly.crs='epsg:4326'
path_data = '.\\data\\' + str(poly_osmid) + '\\' + str_date

path_res = path_data + '\\res'
path_res_edges = path_res
path_res_nodes = path_res

# оставить только те ребра, которые внутри полигона
try:
    gdf_lines = gpd.sjoin(gdf_lines, gdf_poly[['geometry']], how='inner', 
                          op='intersects').drop("index_right", axis=1).reset_index(drop=True)
except:
    pass

# file_name = 'petr_kamch'
# read_shp = gpd.read_file(r'./shp/raw/{}.shp'.format(file_name), encoding = 'utf-8', errors='ignore')



# удаление кривых символов в строке (которые не преобразовались по unicode)
# city_graph = read_shp.copy()
city_graph = gdf_lines.copy()

list_columns = [city_graph['name'], city_graph['other_tags']]
list_new_columns = []
for column in (list_columns):
    list_strings = []
    for row in (column):
        new_row = row
        if (isinstance(bytes(), type(row)) == True):
            list_new_values = []
            j=0
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
            i=0
            for i in list_new_values:
                new_string = new_string+i
            #if (len(new_string.encode('utf-8')) >= 254): 
                # максимальная длина строки в shp - 255
                #new_string = new_string[:254]
            new_row = new_string + '"' #
        list_strings.append(new_row)
#
    list_new_columns.append(list_strings)
# 
# len(list_new_columns[0])
city_graph['name'] = list_new_columns[0]
city_graph['other_tags'] = list_new_columns[1]


# до фильтрации сохранить нужные ребра трамвайных и жд путей
rail_tram = city_graph[(city_graph['other_tags'].str.contains('"railway"=>"tram"', na=False))]
rail_main = city_graph[(city_graph['other_tags'].str.contains('"railway"=>"rail"', na=False))]
rail_main = rail_main[((rail_main['other_tags'].str.contains('"usage"=>"main"', na=False)) 
                       | (rail_main['other_tags'].str.contains('"usage"=>"branch"', na=False))
                       | ((~rail_main['other_tags'].str.contains('service"=>"', na=False)) 
                          & ((~rail_main['other_tags'].str.contains('usage"=>"', na=False)))))]
#
rail_subw = city_graph[(city_graph['other_tags'].str.contains('"railway"=>"subway"', na=False))]
rail_subw = rail_subw[~(rail_subw['other_tags'].str.contains('service', na=False))]


# удаление строк, содержаших ненужные значения
#  списки ненужных значений, которые надо будет удалить
lst_highway_notok = ['steps', 'pedestrian', 'footway', 'path', 'raceway', 'road', 'track', 'planned', 'proposed', 'cycleway']

lst_ot_notok = ['access"=>"no','abandoned','admin_level','aeroway','attraction','building',
                'ferry','grass','hiking', 'ice_road','land','leaf_type',
                'leisure','mud','natural','piste',
                'planned','power','private','proposed','wood','wokrset']
# some are ok: unpaved,description


city_graph = city_graph[
    (city_graph.waterway.isna())
    & (city_graph.aerialway.isna())
    & (city_graph.man_made.isna())
    & ((city_graph.barrier.isna()) | (city_graph['barrier'] == 'yes'))
    & (~city_graph.highway.isin(lst_highway_notok))
    & (~city_graph['other_tags'].str.contains('|'.join(lst_ot_notok), na=False))
    & (~(city_graph['other_tags'].str.contains("sand", na=False) & city_graph.name.isna()))
    & (~((city_graph.z_order == 0) & (city_graph.name.isna())))
    & (~((city_graph.highway == 'construction') 
          & (city_graph.other_tags.isna()) & (city_graph.name.isna())))
    & (~((city_graph.highway == 'service') & (city_graph.name.isna())))
                             ].reset_index(drop=True)
#
#############################
# constructions - temporary changes in the road 
# no name and no tags - new road, should be deleted
constr = city_graph[(((city_graph.highway == 'construction') 
                          & ~(city_graph.other_tags.isna()) 
                          & ~(city_graph.name.isna())) 
                         | ((city_graph.other_tags.str.contains("bridge", na=False)) 
                            & (~(city_graph.name.isna()))
                            & ((city_graph.highway.isna()))))].reset_index(drop=True)
#

lst_contstr_name=list(constr.name.unique())
i=0
for i in range(len(lst_contstr_name)):
    one_name = lst_contstr_name[i]
    df_small = constr[constr.name == one_name].reset_index(drop=True)
    sj_df = gpd.sjoin(df_small, city_graph[['highway', 'name', 'geometry']], 
                      how='inner', op='intersects').drop("index_right", axis=1)
    lst_hw = list(sj_df[((sj_df.name_right == one_name) 
                         & (sj_df.highway_right != 'construction') 
                         & ((sj_df.highway_right.astype(str) != 'None')))].highway_right.unique())
    try:
        new_hw = lst_hw[0]
        for j in range(len(df_small)):
            ind_oi = list(city_graph.osm_id).index(df_small.osm_id[j])
            city_graph.highway[ind_oi] = new_hw
    except:
        pass
# 
#############################

# удаление ненужных и добавление нужных жд и трамвайных путей
city_graph = city_graph[
    (~city_graph['other_tags'].str.contains('=>"rail"', na=False))
    & (~city_graph['other_tags'].str.contains('railway', na=False))
                     ]
city_graph = city_graph.append(rail_tram)
city_graph = city_graph.append(rail_main)
city_graph = city_graph.append(rail_subw)

#city_graph = city_graph[(~city_graph['other_tags'].str.contains('disused', na=False))]

city_graph = city_graph.reset_index(drop=True)



# select lines which are used in routes of public transport
try:
    buff_gdf_multilines = gdf_multilines.to_crs('epsg:32637').buffer(0.5).to_crs('epsg:4326')
    buff_gdf_multilines = gpd.GeoDataFrame(geometry=buff_gdf_multilines)
    inter_gdf_lines =  gpd.sjoin(gdf_lines, buff_gdf_multilines, how='inner', 
                                 op='within').drop("index_right", axis=1).reset_index(drop=True)

    add_new_tmp = inter_gdf_lines[~inter_gdf_lines.osm_id.isin(list(city_graph.osm_id))] #add only new lines

    add_new = gdf_lines[gdf_lines.osm_id.isin(list(add_new_tmp.osm_id))]
    add_new = add_new[
        (add_new.waterway.isna())
        & (add_new.aerialway.isna())
        & (add_new.barrier.isna())
        & (add_new.man_made.isna()) 
        & ~add_new.highway.isin(lst_highway_notok) 
        & (~add_new['other_tags'].str.contains('|'.join(lst_ot_notok), 
                                               na=False))].drop_duplicates()
#
    city_graph = city_graph.append(add_new).reset_index(drop=True)
except:
    pass
#

########################
# изменение направления ребра для oneway=-1
reverse_oneway = city_graph[city_graph.other_tags.str.contains('oneway"=>"-1', 
                                                               na=False)].reset_index(drop=True)
#
lst_geo_new=[]
i=0
for i in range(len(city_graph)):
    if city_graph.osm_id[i] in list(reverse_oneway.osm_id):
        list_geo = list(city_graph.geometry[i].coords[:])
        one_reversed_geo = list_geo[::-1]
        line_2 = LineString(one_reversed_geo)
        lst_geo_new.append(line_2)
    else:
        lst_geo_new.append(city_graph.geometry[i])
# 
try:
    city_graph['geometry'] = lst_geo_new
except:
    print("Error_rev")
# 
########################
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


# res_graph.head(2)

# подтягивание полей с информацией по пересечению геометрий
graph_info = gpd.sjoin(res_graph, city_graph, how='left', 
                           op='within').drop("index_right", axis=1).reset_index(drop=True)
#
#################################
nans_g = graph_info[graph_info.osm_id.isna()]
nans_g = gpd.sjoin(nans_g[['geometry']], city_graph, how='left', 
                           op='intersects').drop("index_right", axis=1).reset_index(drop=True)

# эти необходимо пере_разбить
tmp_city = city_graph[city_graph.osm_id.isin(nans_g.osm_id)].reset_index(drop=True)

# удалить пустые и те, которые необходимо пере_разбить, остальные - ок
good_graph_info = graph_info[((~graph_info.osm_id.isna()) 
                              & (~graph_info.osm_id.isin(tmp_city.osm_id)))].reset_index(drop=True)
#
lst_uu_geo = []
newlst=[]
i=0
for i in (range(len(tmp_city))):
# for i in tqdm(range(5)):
    one_geo = tmp_city.iloc[[i]]
    tmp_sj = gpd.sjoin(one_geo[['geometry']], city_graph, how='inner', 
                       op='intersects').drop("index_right", axis=1).reset_index(drop=True)
    sj_one = city_graph[city_graph.osm_id.isin(tmp_sj.osm_id)].reset_index(drop=True)
    lst_one_geo = tmp_city.geometry[i].coords[:]
    uniqlines = []
    lst_sj_ends=[]
    j=0
    for j in range(len(sj_one)):
        lst_uu = []
        tmp_lst = []
        tmp_lst.append(sj_one.geometry[j])
        lst_sj_one_geo = sj_one.geometry[j].coords[:]
        res = list(set(lst_one_geo) & set(lst_sj_one_geo)) #find mutual points
        if len(res) > 0:
            for k in res:
                if ((k != tmp_city.geometry[i].coords[0]) & (k != tmp_city.geometry[i].coords[-1])):
                    if sj_one.geometry[j] not in lst_uu:
                        lst_uu.append(sj_one.geometry[j])
                if ((k == sj_one.geometry[j].coords[0]) | (k == sj_one.geometry[j].coords[-1])):
                    if sj_one.geometry[j] not in lst_sj_ends:
                        lst_sj_ends.append(sj_one.geometry[j])
        #
        for line in lst_uu:
            if not any(p.equals(line) for p in uniqlines):
                uniqlines.append(line)
    if tmp_city.geometry[i] not in uniqlines:
            uniqlines.append(tmp_city.geometry[i])
    if len(uniqlines) > 1:
        uu_geo = unary_union(uniqlines)
        one_gdf = gpd.GeoDataFrame(geometry=list(uu_geo))
        one_gdf.crs='epsg:4326'
        tmp_one_gdf = gpd.sjoin(one_gdf, city_graph, how='left', 
                                op='within').drop("index_right", axis=1)
        if len(tmp_one_gdf[tmp_one_gdf.osm_id.isna()]) > 0:
            line_f = tmp_city.geometry[i]
            line = tmp_city.geometry[i]
            d=0
            cnt=0
            for d in range(len(lst_sj_ends)):
                try:
                    point = Point(list(set(line_f.coords[:]) & set(lst_sj_ends[d].coords[:]))[0])
                    if (((point.coords[0] != line_f.coords[0]) & (point.coords[0] != line_f.coords[-1])) 
                        & ((point.coords[0] == lst_sj_ends[d].coords[0]) | (point.coords[0] == lst_sj_ends[d].coords[-1]))):
                        new_geo = shapely.geometry.MultiLineString(list(shapely.ops.split(line,point)))
                        line = new_geo
                        cnt+=1
                except:
                    pass
            if cnt > 0:
                newlst.append(new_geo)
            else:
                new_geo = tmp_city.geometry[i]
        else:
            new_geo = shapely.geometry.MultiLineString(list(tmp_one_gdf[tmp_one_gdf.osm_id 
                                                                    == tmp_city.osm_id[i]].geometry))
    else:
        new_geo = tmp_city.geometry[i]
    #
    lst_uu_geo.append(new_geo)
# 
try:
    tmp_city['uu_geo'] = lst_uu_geo
except:
    print("Error_uu")
#
new_df = []
i=0
for i in (range(len(tmp_city))):
    
    one_line = tmp_city.uu_geo[i]
    try:
        len_ol = len(one_line)
        j = 0
        for j in range(len_ol):
            lst_one = list(tmp_city.iloc[i][:-1])
            lst_one.append(one_line[j])
            new_df.append(lst_one)
    except:
        new_df.append(list(tmp_city.iloc[i]))
# 

new_gdf = gpd.GeoDataFrame(columns=tmp_city.columns, data=new_df)
del new_gdf['geometry']
new_gdf = new_gdf.rename(columns={'uu_geo':'geometry'})
new_gdf.crs='epsg:4326'

tr_gi = good_graph_info.append(new_gdf).reset_index(drop=True)
graph_filtrd = tr_gi.copy()
graph_filtrd['z_order'] = graph_filtrd['z_order'].astype(np.int64)
#################################

##############

def make_graph_great_again(gdf):
    G = momepy.gdf_to_nx(gdf, approach='primal')
    
    # выбор наибольшего графа из подграфов 
    # (когда ребра удаляются выше, остаются подвешенные куски графа, их надо удалить)
     # whatever graph you're working with
    cur_graph = G
#     def del_subgraphs(cur_graph):
    list_subgraphs = [cur_graph]
    if not nx.is_connected(cur_graph):
        # get a list of unconnected networks
        def connected_component_subgraphs(cur_graph):
            for c in nx.connected_components(cur_graph):
                yield cur_graph.subgraph(c)
        sub_graphs = connected_component_subgraphs(cur_graph)
        list_graph = []
        i=0
        for i in sub_graphs:
            list_graph.append(i)

        main_graph = list_graph[0]
        list_subgraphs = []
        #list_subgraphs.append(main_graph)

        # find the largest network in that list
        for sg in list_graph:
            if len(sg.nodes()) > len(main_graph.nodes()):
                main_graph = sg
            else:
                list_subgraphs.append(sg)
        try:
            list_subgraphs.remove(main_graph)
        except:
            pass
        cur_graph = main_graph
        #print(nx.info(cur_graph))
#         return cur_graph, list_subgraphs
    # del subgraphs if exist
#     cur_graph, list_subgraphs = del_subgraphs(G)
    #####
    
    sub_graph = cur_graph
    if len(list_subgraphs) > 1:
        sub_graph = nx.MultiGraph()
        i=0
        for i in range(len(list_subgraphs)):
            H = list_subgraphs[i]
            sub_graph.add_edges_from(H.edges(data=True))
            sub_graph.add_nodes_from(H.nodes(data=True))
    #create gdfs
    # формирование таблиц из графа и узлов (nodes)
    nodes, new_graph = momepy.nx_to_gdf(cur_graph)
    nodes.crs='epsg:4326'
    new_graph.crs='epsg:4326'
    
    sub_nodes, sub_graph = momepy.nx_to_gdf(sub_graph)
    sub_graph.crs='epsg:4326'
    sub_nodes.crs='epsg:4326'

    return nodes, new_graph, sub_graph

nodes, new_graph, sub_graph = make_graph_great_again(graph_filtrd)

#################################################

#################################################
# создание digraph (двунаправленного графа)

###### Create Reverse of graph

direct_gdf = new_graph[['osm_id', 'name', 'highway', 'z_order', 'other_tags', 'geometry']].copy()
reverse_gdf = direct_gdf.copy()
reversed_geo_all = []
i=0
for i in (range(len(reverse_gdf))):
    list_geo = list(reverse_gdf.geometry[i].coords[:])
    one_reversed_geo = list_geo[::-1]
    line_2 = LineString(one_reversed_geo)
    reversed_geo_all.append(line_2)
# 


rev_geo = gpd.GeoDataFrame(geometry=reversed_geo_all)
try:
    reverse_gdf['rev_geo'] = rev_geo['geometry']
    reverse_gdf = reverse_gdf.rename(columns={'geometry':'old_geo', 'rev_geo':'geometry'})
    reverse_gdf = reverse_gdf[['osm_id', 'name', 'highway', 'z_order', 'other_tags', 'geometry']]
except:
    print("Error!")


direct_gdf['direction'] = "direct"
reverse_gdf['direction'] = "reverse"
all_gdf = direct_gdf.append(reverse_gdf).reset_index(drop=True)
all_gdf.crs = 'epsg:4326'


############################################

# разбиение петель и ребер, где на пару node_from node_to - больше 2 ребер

tmp_grph = all_gdf.copy()
#
lst_petl = []
lst_start = []
lst_end = []
lst_stend = []
i=0
for i in (range(len(tmp_grph))):
    if tmp_grph.geometry[i].coords[0] == tmp_grph.geometry[i].coords[-1]:
        lst_petl.append(tmp_grph.geometry[i])
    else:
        lst_stend.append(str(tmp_grph.geometry[i].coords[0]) + "_" + str(tmp_grph.geometry[i].coords[-1]))
# 

my_dict = {i:lst_stend.count(i) for i in lst_stend}

newDict = {}
for (key, value) in my_dict.items():
    if value > 1:
        newDict[key] = value
#

#
lst_geo = []
lst_len_geo = []
i=0
for i in range(len(tmp_grph)):
    str_st_end = str(tmp_grph.geometry[i].coords[0]) + "_" + str(tmp_grph.geometry[i].coords[-1])
    if str_st_end in (newDict.keys()):
        lst_geo.append(str_st_end)
        lst_len_geo.append(tmp_grph.geometry[i].length)
    else:
        lst_geo.append(None)
        lst_len_geo.append(None)
#

# gdf_to_cut = tmp_grph.copy()
tmp_grph['geo_grup'] = lst_geo
tmp_grph['geo_len'] = lst_len_geo

i=0

dct_gr_len = {}
i=0
for i in range(len(tmp_grph)):
    one_group = tmp_grph.geo_grup[i]
    if one_group not in dct_gr_len.keys():
        lst_gr_len = []
        dct_gr_len[one_group] = lst_gr_len
    dct_gr_len[one_group] = dct_gr_len[one_group] + [tmp_grph.geo_len[i]]  
# 
del dct_gr_len[None]
del tmp_grph['geo_len'], tmp_grph['geo_grup']

lst_max = []
i=0
for i in range(len(tmp_grph)):
    one_group = str(tmp_grph.geometry[i].coords[0]) + "_" + str(tmp_grph.geometry[i].coords[-1])
    if one_group in dct_gr_len.keys():
        if (max(dct_gr_len[one_group]) == tmp_grph.geometry[i].length):
            lst_max.append(1)
        else:
            lst_max.append(None)
    elif tmp_grph.geometry[i].coords[0] == tmp_grph.geometry[i].coords[-1]:
        lst_max.append(1)
    else:
        lst_max.append(None)
# 
try:
    tmp_grph['cut_geo'] = lst_max
except:
    print("Error_cut_double")
#
#########################
# функция обрезки ребер
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
#########################
all_ok = tmp_grph[tmp_grph.cut_geo != 1].reset_index(drop=True)
cut_gdf = tmp_grph[tmp_grph.cut_geo == 1].reset_index(drop=True)

big_gdf = gpd.GeoDataFrame()
i=0
for i in (range(len(cut_gdf))):
    line = cut_gdf.geometry[i]
    lst_one_geo = cut(line, (line.length / 2))
    small_gdf = gpd.GeoDataFrame(geometry=lst_one_geo)
    small_gdf.crs='epsg:4326'
    small_gdf = gpd.sjoin(small_gdf, cut_gdf.iloc[[i]], how='left', 
                            op='intersects').drop("index_right", axis=1)
    #
    big_gdf = big_gdf.append(small_gdf)
# 
big_gdf = all_ok.append(big_gdf).reset_index(drop=True)

del big_gdf['cut_geo']

big_gdf.crs = 'epsg:4326'
big_gdf = big_gdf.to_crs('epsg:32637')
#########################

############################################

all_graph = momepy.gdf_to_nx(big_gdf)
all_nodes, all_edges = momepy.nx_to_gdf(all_graph)

all_edges.crs = 'epsg:32637'
all_edges = all_edges.to_crs('epsg:4326')
all_nodes.crs = 'epsg:32637'
all_nodes = all_nodes.to_crs('epsg:4326')



########### Check node_from and node_to - change if wrong ###########

check_points = all_edges.copy()

check_points = check_points.rename(columns={'geometry': 'line_geometry'})

check_points['nodeID'] = check_points['node_start']
check_points = check_points.merge(all_nodes, how='left', on=['nodeID'])
check_points = check_points.rename(columns={'geometry': 'start_geometry'})

check_points['nodeID'] = check_points['node_end']
check_points = check_points.merge(all_nodes, how='left', on=['nodeID'])
check_points = check_points.rename(columns={'geometry': 'end_geometry'})

del check_points['nodeID']

check_points['start_true'] = None
check_points['end_true'] = None
list_check_start = []
list_check_end = []
i=0
for i in (range(len(check_points))):
    if check_points.start_geometry[i].coords[0] == check_points.line_geometry[i].coords[0]:
        list_check_start.append(check_points.node_start[i])
    elif check_points.end_geometry[i].coords[0] == check_points.line_geometry[i].coords[0]:
        list_check_start.append(check_points.node_end[i])
    else:
        list_check_start.append(None)
# 

for ii in (range(len(check_points))):
    if check_points.end_geometry[ii].coords[0] == check_points.line_geometry[ii].coords[-1]:
        list_check_end.append(check_points.node_end[ii])
    elif check_points.start_geometry[ii].coords[0] == check_points.line_geometry[ii].coords[-1]:
        list_check_end.append(check_points.node_start[ii])
    else:
        list_check_end.append(None)
# 
try:
    check_points['start_true'] = list_check_start
    check_points['end_true'] = list_check_end
except:
    print("Error1")
# 

########### delete oneways reverse #############

ok_oneway = check_points.copy()

ok_oneway = ok_oneway.reset_index(drop=True)
ok_oneway = ok_oneway.reset_index()
ok_oneway = ok_oneway.rename(columns={'index':'link_id'})
ok_oneway['link_id'] = ok_oneway['link_id'] + 1

ok_oneway = ok_oneway[['link_id', 'osm_id', 'name', 'highway', 'z_order', 'other_tags',
                       'line_geometry', 'direction', 'mm_len', 'start_true', 'end_true']]
ok_oneway = ok_oneway.rename(columns={'line_geometry':'geometry', 
                                      'start_true':'node_start', 
                                      'end_true':'node_end'})
graph_full = ok_oneway.copy()
graph_full = gpd.GeoDataFrame(graph_full)
graph_full.crs = 'epsg:4326'

graph_full['z_order'] = graph_full['z_order'].astype(np.int64)
graph_full['mm_len'] = round((graph_full['mm_len'] / 1000), 3)

# create num_lanes column
list_lanes = []
reg = re.compile('[^0-9]')

lst_onw=['oneway"=>"yes','oneway"=>"1','oneway"=>"true', 'oneway"=>"-1']
i=0
for i in range(len(graph_full)):
    str1 = str(graph_full.other_tags[i])
    if any((c in str1) for c in lst_onw):
        if graph_full.direction[i] == 'direct':
            if '"lanes"=>"' in str1:
                str2 = str1[str1.find('"lanes"=>"') : ].split(",", 1)[0]
                int_lanes = int(reg.sub('', str2))
                list_lanes.append(int_lanes)
            else:
                list_lanes.append(1)
        else:
            list_lanes.append(0)
    else:
        if '"lanes"=>"' in str1:
            str2 = str1[str1.find('"lanes"=>"') : ].split(",", 1)[0]
            int_lanes = int(reg.sub('', str2))
            if int_lanes > 1:
                if graph_full.direction[i] == 'direct':
                    list_lanes.append(math.ceil(int_lanes/2))
                else:
                    list_lanes.append(math.floor(int_lanes/2))
            else:
                list_lanes.append(1)
        else:
            list_lanes.append(1)
# 
try:
    graph_full['NUMLANES'] = list_lanes
except:
    print("Error2")
#  

lst_types = []

i=0
for i in (range(len(graph_full))):
    if "railway" in str(graph_full.other_tags[i]):
        if '=>"tram"' in str(graph_full.other_tags[i]):
            if 'surface' in str(graph_full.other_tags[i]):
                lst_types.append("TM,CAR,BUS,TB,MT")
            else:
                lst_types.append("TM")
        elif 'subway' in str(graph_full.other_tags[i]):
            lst_types.append("MTR")
        else:
            lst_types.append("E")
    else:
        if (
            ('psv"=>"only"' in str(graph_full.other_tags[i]))
            |
            (('psv"=>"yes"' in str(graph_full.other_tags[i]))
                & ('vehicle"=>"no"' in str(graph_full.other_tags[i])))):
            lst_types.append("BUS,TB,MT")
        else:
            lst_types.append("CAR,BUS,TB,MT")
#  

try:
    graph_full['TSYSSET'] = lst_types
except:
    print("Error3")
# 

##############################
# add type link

lst_typeno = []
i=0
for i in range(len(graph_full)):
    if "railway" in str(graph_full.other_tags[i]):
        if "subway" in str(graph_full.other_tags[i]):
            lst_typeno.append(10)
        elif "tram" in str(graph_full.other_tags[i]):
            lst_typeno.append(40)
        else:
            lst_typeno.append(20)
    elif graph_full.NUMLANES[i] == 0:
        lst_typeno.append(0)
    else:
        if graph_full.highway[i] in ['motorway','trunk']:
            lst_typeno.append(1)
        elif graph_full.highway[i] == 'primary':
            lst_typeno.append(2)
        elif graph_full.highway[i] == 'secondary':
            lst_typeno.append(3)
        elif graph_full.highway[i] == 'tertiary':
            lst_typeno.append(4)
        elif graph_full.highway[i] in ['motorway_link','trunk_link', 'primary_link', 'secondary_link', 'tertiary_link']:
            lst_typeno.append(5)
        elif graph_full.name[i] != None:
            lst_typeno.append(6)
        else:
            lst_typeno.append(7)
# 

try:
    graph_full['TYPENO_2'] = lst_typeno
except:
    print("Error_typeno")

##############################

graph_full.crs='epsg:4326'
graph_full = graph_full.rename(columns={'link_id':'NO', 'mm_len':'LENGTH', 
                                        'node_start':'FROMNODENO', 'node_end':'TONODENO'})
#
#
######### End of graph checking #############

#################################################

# обрезать для сохранения в шейп

i=0
graph_full_shp = graph_full.copy()
for i in range(len(graph_full_shp)):
    if len(str(graph_full_shp.other_tags[i])) > 254:
        graph_full_shp.other_tags[i] = graph_full_shp.other_tags[i][:254]
# 
graph_full_shp.to_file('{}\\new_graph_{}_{}_{}.shp'.format(path_res_edges,buff_km, place, str_date), encoding='utf-8')

all_nodes = all_nodes.rename(columns={'nodeID':'NO'})
all_nodes['XCOORD'] = all_nodes.geometry.x
all_nodes['YCOORD'] = all_nodes.geometry.y
all_nodes.to_file('{}\\nodes_{}_{}_{}.shp'.format(path_res_nodes,buff_km, place, str_date), encoding='utf-8')
print("Results are in folder 'res'")
print("Done")