#############################
import gdf_from_osm
#############################

print()
print("___________________________")
print("Part 3. Processing a graph (filtering and creating nodes)")
print("Please wait...")
print()

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
import osmnx as ox
import re
import math


gdf_lines = gdf_from_osm.gdf_lines
gdf_multilines = gdf_from_osm.gdf_multilines
str_date = gdf_from_osm.str_date
place = gdf_from_osm.place
buff_km = gdf_from_osm.buff_km

# BORDERS!!!!!
gdf_poly = gdf_from_osm.gdf_poly
# gdf_poly = ox.gdf_from_place(place, which_result=2, buffer_dist=int(buff_km)*1000)
# gdf_poly.crs='epsg:4326'



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
rail_tram = city_graph[(city_graph['other_tags'].str.contains('=>"tram"', na=False))]
rail_main = city_graph[(city_graph['other_tags'].str.contains('"usage"=>"main"', na=False))]

# удаление строк, содержаших ненужные значения
#  списки ненужных значений, которые надо будет удалить
lst_highway_notok = ['steps', 'pedestrian', 'footway', 'path', 'raceway', 'road', 'track', 'planned', 'proposed']

lst_ot_notok = ['admin_level','aeroway','attraction','building','ferry','grass','land','leaf_type',
                'leisure','mud','natural','piste','planned','power','private','proposed','wood']
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
    & (~((city_graph.highway == 'service') & (city_graph.name.isna())))
                             ].reset_index(drop=True)
#

# удаление ненужных и добавление нужных жд и трамвайных путей
city_graph = city_graph[
    (~city_graph['other_tags'].str.contains('=>"rail"', na=False))
    & (~city_graph['other_tags'].str.contains('railway', na=False))
                     ]
city_graph = city_graph.append(rail_tram)
city_graph = city_graph.append(rail_main)

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
        & (add_new.man_made.isna())].drop_duplicates()

    city_graph = city_graph.append(add_new).reset_index(drop=True)
except:
    pass

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
    Needs cut(line,distance) func from above
    """
    precut = cut(line, start)[0]
    #result = cut(precut, end)[1]
    try:
        result = cut(precut, end)[1]
    except:
        result = precut
    return result

def cut_geo(gdf):
    gdf = gdf.copy()
    # вызов функции обрезки
    list_new_geo = []

    list_geo_old = list(gdf.to_crs('epsg:32637').geometry) 
    # преобразование - чтобы в метрах отсечь
    i=0
    for i in range(len(list_geo_old)):
        line = list_geo_old[i]
        length_line = line.length
        start = length_line - 0.05 # это длина в метрах - 0.05 метров
        end = length_line - start
        cutted_line = cut_piece(line, start, end)
        list_new_geo.append(cutted_line)
    # 

    gdf['new_geo'] = list_new_geo
    gdf = gdf.rename(columns={'geometry':'old_geo', 'new_geo':'geometry'})
    del gdf['old_geo']

    gdf.crs = 'epsg:32637'
    gdf = gdf.to_crs('epsg:4326')
    return gdf


# вызов функции обрезки
cut_nans_gdf = graph_info[graph_info.osm_id.isna()].copy().reset_index(drop=True)
copy_nans_gdf = cut_geo(cut_nans_gdf)

#  здесь реализован поиск многоуровневых эстакад (ребер, и их id)

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


# разбиение ребер, которые пересекают эстакады
# но не все между собой, а только те, 
# которые не входят в эстакаду, потому что ее надо оставить целостной
list_nans = list(nans_gdf_inter[~nans_gdf_inter.osm_id.isna()].osm_id.unique())

for_one_nan = gpd.GeoDataFrame()


for osmid in (range(len(list_nans))):
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
    # 
    graph_one_info = graph_one_info[graph_one_info.osm_id == osm_id_one].reset_index(drop=True)
    # 


    for_one_nan = for_one_nan.append(graph_one_info).reset_index(drop=True)
#

for_one_nan.head(2)

# объединение все ребер в одни граф, кроме пустых значений и дубликатов
list_for_one = list(for_one_nan[~for_one_nan.osm_id.isna()].osm_id.unique())
not_nan_graph = graph_info.copy()
not_nan_graph = not_nan_graph[(~not_nan_graph.osm_id.isna()) 
                              & (~not_nan_graph.osm_id.isin(list_for_one))
                             ]
graph_filtrd = not_nan_graph.append(for_one_nan).reset_index(drop=True)
graph_filtrd['z_order'] = graph_filtrd['z_order'].astype(np.int64)

def make_graph_great_again(gdf):
    G = momepy.gdf_to_nx(gdf, approach='primal')
    
    # выбор наибольшего графа из подграфов 
    # (когда ребра удаляются выше, остаются подвешенные куски графа, их надо удалить)
     # whatever graph you're working with

    def del_subgraphs(cur_graph):
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
            return cur_graph, list_subgraphs
    # del subgraphs if exist
    cur_graph, list_subgraphs = del_subgraphs(G)
    sub_graph = cur_graph
    if len(list_subgraphs) > 1:
        ng = nx.MultiGraph()
        i=0
        for i in range(len(list_subgraphs)):
            H = list_subgraphs[i]
            ng.add_edges_from(H.edges(data=True))
            ng.add_nodes_from(H.nodes(data=True))
        sub_nodes, sub_graph = momepy.nx_to_gdf(ng)
        sub_graph.crs='epsg:4326'
        sub_nodes.crs='epsg:4326'
    #create gdfs
    # формирование таблиц из графа и узлов (nodes)
    nodes, new_graph = momepy.nx_to_gdf(cur_graph)

    nodes.crs='epsg:4326'
    new_graph.crs='epsg:4326'
    return nodes, new_graph, sub_graph

nodes, new_graph, sub_graph = make_graph_great_again(graph_filtrd)

# некоторые ребра могут не пересечься и поэтому останутся подвешенными, хотя визуально накладываются друг на друга
# это происходит в случаях, когда только одно пересечение, и оно не сработало (не подтянулись поля)
# поэтому их надо пере_пересечь
if ((len(sub_graph) > 0) & (len(new_graph) != len(sub_graph))):
    # создание точек, для того, чтобы пересечь только конечные, а не всю линию
    list_points = []
    i=0
    for i in range(len(sub_graph)):
        list_points.append(Point(sub_graph.geometry[i].coords[0]))
        list_points.append(Point(sub_graph.geometry[i].coords[-1]))
    # 
    tmp_ponts = gpd.GeoDataFrame(geometry=list_points)
    tmp_ponts.crs='epsg:4326'
    
    buff_tmp = tmp_ponts.copy()
    buff_tmp = buff_tmp.to_crs('epsg:32637').buffer(0.5).to_crs('epsg:4326')
    buff_tmp = gpd.GeoDataFrame(geometry=buff_tmp)
    
    tmp_graph =  gpd.sjoin(graph_filtrd, buff_tmp, how='inner', 
                                 op='intersects').drop("index_right", axis=1).reset_index(drop=True)
    tmp_graph = tmp_graph.drop_duplicates(list(tmp_graph.columns)).reset_index(drop=True)
    
    tmp_uu = gpd.GeoDataFrame(geometry = list(unary_union(list(tmp_graph.geometry))))
    tmp_uu.crs='epsg:4326'
    # # подтягивание полей с информацией по пересечению геометрий
    tmp_uu_info = gpd.sjoin(tmp_uu, tmp_graph, how='left', 
                               op='within').drop("index_right", axis=1).reset_index(drop=True)
    #####
    nans_sub = tmp_uu_info[tmp_uu_info.osm_id.isna()].reset_index(drop=True)
    if len(nans_sub) > 0:
        nans_sub = nans_sub[['geometry']]
        nans_sub = nans_sub.reset_index().rename(columns={'index':'sub_link'})
        nans_sub['sub_link'] = nans_sub['sub_link'] + 1

        ####
        copy_nans_sub = cut_geo(nans_sub)
        copy_nans_sub = copy_nans_sub.to_crs('epsg:32637')
        copy_nans_sub['geometry'] = copy_nans_sub['geometry'].buffer(0.003)
        copy_nans_sub = copy_nans_sub.to_crs('epsg:4326')

        ####
        inter_tmp_1 = gpd.sjoin(copy_nans_sub, tmp_graph, how='left', 
                                   op='intersects').drop("index_right", axis=1).reset_index(drop=True)
        fill_nan = nans_sub.merge(inter_tmp_1[['sub_link','osm_id', 'name', 'highway', 'waterway',
               'aerialway', 'barrier', 'man_made', 'z_order', 'other_tags']], how='left', on=['sub_link'])
        sub_new = tmp_uu_info[~tmp_uu_info.osm_id.isna()][['geometry', 'osm_id', 'name', 'highway', 'waterway', 'aerialway',
               'barrier', 'man_made', 'z_order', 'other_tags']]
        sub_new = sub_new.append(fill_nan[['geometry', 'osm_id', 'name', 'highway', 'waterway', 'aerialway',
               'barrier', 'man_made', 'z_order', 'other_tags']]).reset_index(drop=True)
    else:
        sub_new = tmp_uu_info.copy()
    ###########
    gf_copy = graph_filtrd.copy()
    gf_copy['str_geo'] = gf_copy['geometry'].astype(str)
    tmp_graph['str_geo'] = tmp_graph['geometry'].astype(str)
    gf_copy = gf_copy[~gf_copy.str_geo.isin(tmp_graph.str_geo)]
    gf_copy = gf_copy.append(sub_new).reset_index(drop=True)
    del gf_copy['str_geo']

    nodes, new_graph, sub_graph = make_graph_great_again(gf_copy)
#
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

df_dict_double = pd.DataFrame(columns = ['str_st_end', 'cnt'])
df_dict_double['str_st_end'] = newDict.keys()
df_dict_double['cnt'] = newDict.values()

lst_str_st = []
lst_str_end = []
i=0
for i in range(len(df_dict_double)):
    p_start, p_end = df_dict_double['str_st_end'][i].split("_")
    lst_str_st.append(p_start)
    lst_str_end.append(p_end)
# 
df_dict_double['p_start'], df_dict_double['p_end'] = lst_str_st, lst_str_end

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

big_gdf = gpd.GeoDataFrame()
i=0
for i in (range(len(tmp_grph))):
    if tmp_grph.cut_geo[i] == 1:
        lst_one_geo = cut(tmp_grph.geometry[i], (tmp_grph.geometry[i].length / 2))
        small_gdf = gpd.GeoDataFrame(geometry=lst_one_geo)
        small_gdf.crs='epsg:4326'
        small_gdf = gpd.sjoin(small_gdf, tmp_grph.iloc[[i]], how='left', 
                                op='intersects').drop("index_right", axis=1)

    else:
        small_gdf = tmp_grph.iloc[[i]]
    big_gdf = big_gdf.append(small_gdf)
# 
big_gdf = big_gdf.reset_index(drop=True)
del big_gdf['cut_geo']

#big_gdf.to_file("big_gdf.shp", encoding='utf-8')

big_gdf.crs = 'epsg:4326'
big_gdf = big_gdf.to_crs('epsg:32637')
############################################

all_graph = momepy.gdf_to_nx(big_gdf)
all_nodes, all_edges = momepy.nx_to_gdf(all_graph)
#print(nx.info(all_graph))

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

# two_way = check_points[
    # (~check_points['other_tags'].str.contains('"oneway"=>"yes"', na=False))
    # & (~check_points['other_tags'].str.contains('"oneway"=>"1"', na=False))]
# single_oneway = check_points[
    # ((check_points['other_tags'].str.contains('"oneway"=>"yes"', na=False))
    # | (check_points['other_tags'].str.contains('"oneway"=>"1"', na=False)))
    # & (check_points['direction'] == 'direct')]
# ok_oneway = two_way.append(single_oneway).reset_index(drop=True)
# #


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
i=0
for i in (range(len(graph_full))):
    if (('oneway"=>"yes' in str(graph_full.other_tags[i])) 
            | ('oneway"=>"1' in str(graph_full.other_tags[i]))):
            if graph_full.direction[i] == 'direct':
                list_lanes.append(int_lanes)
            else:
                list_lanes.append(0)
    else:
        if '"lanes"=>"' in str(graph_full.other_tags[i]):
            str1 = str(graph_full.other_tags[i])
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

lst_etrain_nodes = []
lst_tram_nodes = []
lst_psv_nodes = []
lst_all_veh_nodes = []
lst_car_nodes = []

i=0
for i in (range(len(graph_full))):
    if '=>"tram"' in str(graph_full.other_tags[i]):
        lst_types.append("T")
        lst_tram_nodes.append(graph_full.node_start[i])
        lst_tram_nodes.append(graph_full.node_end[i])
    else:
        if '"usage"=>"main"' in str(graph_full.other_tags[i]):
            lst_types.append("E")
            lst_etrain_nodes.append(graph_full.node_start[i])
            lst_etrain_nodes.append(graph_full.node_end[i])
        else:
            if (
                ('psv"=>"only"' in str(graph_full.other_tags[i]))
                |
                (
                    ('psv"=>"yes"' in str(graph_full.other_tags[i]))
                    &
                    ('vehicle"=>"no"' in str(graph_full.other_tags[i]))
                )
            ):
                lst_types.append("BUS,TB,MT")
                lst_psv_nodes.append(graph_full.node_start[i])
                lst_psv_nodes.append(graph_full.node_end[i])
            else:
#                 if (
#                     ('psv"=>"no"' in str(graph_full.other_tags[i]))
#                     &
#                     ('vehicle"=>"no"' not in str(graph_full.other_tags[i]))
#                     &
#                     ('vehicle"=>"' in str(graph_full.other_tags[i]))
#                 ):
#                     lst_types.append("CAR")
#                     lst_car_nodes.append(graph_full.node_start[i])
#                     lst_car_nodes.append(graph_full.node_end[i])
#                 else:
                lst_types.append("CAR,BUS,TB,MT")
                lst_all_veh_nodes.append(graph_full.node_start[i])
                lst_all_veh_nodes.append(graph_full.node_end[i])
# 

try:
    graph_full['TSYSSET'] = lst_types
except:
    print("Error3")
# 
lst_etrain_nodes = list(set(lst_etrain_nodes))
lst_tram_nodes = list(set(lst_tram_nodes))
lst_psv_nodes = list(set(lst_psv_nodes))
lst_all_veh_nodes = list(set(lst_all_veh_nodes))
# lst_car_nodes = list(set(lst_car_nodes))


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
graph_full_shp.to_file('./data/res/edges/new_graph_{}_{}_{}.shp'.format(buff_km, place, str_date), encoding='utf-8')

all_nodes = all_nodes.rename(columns={'nodeID':'NO'})
all_nodes.to_file('./data/res/nodes/nodes_{}_{}_{}.shp'.format(buff_km, place, str_date), encoding='utf-8')

print("Done")