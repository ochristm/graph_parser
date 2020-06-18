import parse_api

#############################
print()
print("___________________________")
print("Part 2. Read osm file and save layers to gdfs_shps.")
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

# import networkx as nx
# import osmnx as ox
import pandas as pd
import geopandas as gpd
import shapely
import shapely.wkt
from shapely.geometry import Point, LineString, MultiLineString, Polygon
#from tqdm.notebook import tqdm

#import wget
#from osgeo import gdal, ogr

#import momepy
#from datetime import datetime
import re
# import girs
# from girs.feat.layers import LayersReader
# import os.path
# import overpass

#отключить предупреждения pandas (так быстрее считает!!!):
pd.options.mode.chained_assignment = None

import warnings
warnings.filterwarnings("ignore")
#############################



ways = parse_api.ways
poly_resp = parse_api.poly_resp
str_date = parse_api.str_date
place = parse_api.place
buff_km = parse_api.buff_km
poly_osmid = parse_api.poly_osmid

################
# path_data='./data/'
# path_raw_csv = path_data
# path_raw_shp_layers = path_data
# path_raw_shp_poly = path_data

path_data = '.\\data\\' + str(poly_osmid) + '\\' + str_date
path_raw = path_data + '\\raw'
path_raw_csv = path_raw
path_raw_shp_layers = path_raw + '\\layers'
path_raw_shp_poly = path_raw_shp_layers

################


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
            small_lst.append(element['tags']['name'])
        except:
            small_lst.append(None)
        try:
            one_tags = element['tags']
            try:
                del one_tags['highway']
            except:
                pass
            try:
                del one_tags['name']
            except:
                pass
            str_tgs = str(one_tags)
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

gdf_lines = gpd.GeoDataFrame(data=big_lst, columns=['osm_id', 'highway', 'name', 'other_tags', 'geometry'])
gdf_lines['osm_id'] = gdf_lines['osm_id'].astype(str)
gdf_lines.crs='epsg:4326'

lst_columns = ['waterway', 'aerialway', 'barrier', 'man_made']
for clm in lst_columns:
    if clm not in list(gdf_lines.columns):
        gdf_lines[clm] = None
#

# при импорте скрипта для чтения с ОСМ - колонка z_order генерится сама
# без импорта - ниже функция создания этого поля
def CreateZorderColumn(gdf_lines):
    np_gdfl = gdf_lines.to_numpy()
    ind_oi = list(gdf_lines.columns).index('osm_id')
    ind_hw = list(gdf_lines.columns).index('highway')
    ind_ot = list(gdf_lines.columns).index('other_tags')
    
    reg = re.compile('[^0-9-]') # only ints
    dict_z_order = {}
    i=0
    for i in range(len(np_gdfl)):
        osmid = np_gdfl[i,ind_oi]
#         osmid = gdf_lines.osm_id[i]
        dict_z_order[osmid] = 0
        hw_str = np_gdfl[i,ind_hw]
        
        if hw_str in ['minor','road','unclassified','residential']:
            dict_z_order[osmid] = 3
        elif hw_str in ['tertiary_link','tertiary']:
            dict_z_order[osmid] = 4
        elif hw_str in ['secondary_link','secondary']:
            dict_z_order[osmid] = 6
        elif hw_str in ['primary_link','primary']:
            dict_z_order[osmid] = 7
        elif hw_str in ['trunk_link','trunk']:
            dict_z_order[osmid] = 8
        elif hw_str in ['motorway_link','motorway']:
            dict_z_order[osmid] = 9
        else:
            dict_z_order[osmid] = 0
        #
        ot_str = np_gdfl[i,ind_ot]
        if (('"bridge"=>"yes"' in str(ot_str))
        or
        ('"bridge"=>"true"' in str(ot_str))
        or
        ('"bridge"=>"1"' in str(ot_str))):
            dict_z_order[osmid] = dict_z_order[osmid] + 10
        #
        if (('"tunnel"=>"yes"' in str(ot_str))
        or
        ('"tunnel"=>"true"' in str(ot_str))
        or
        ('"tunnel"=>"1"' in str(ot_str))):
            dict_z_order[osmid] = dict_z_order[osmid] - 10
        #
        if ('railway' in str(ot_str)):
            dict_z_order[osmid] = dict_z_order[osmid] + 5
        #
        if ('layer' in str(ot_str)):

            str1 = ot_str
            str2 = str1[str1.find('"layer"=>"') : ].split(",", 1)[0]
            int_layer = int(reg.sub('', str2))
            dict_z_order[osmid] = dict_z_order[osmid] + (10*int_layer)
        #
    # 
    df_dict = pd.DataFrame(columns=['osm_id', 'z_order'])
    df_dict['osm_id'] = list(dict_z_order.keys())
    df_dict['z_order'] = list(dict_z_order.values())
    # если значения будут расходиться, можно проверить с изначальным графом
    # read_shp = gpd.read_file(r'./for_exp/lines_yaroslavl.shp', encoding = 'utf-8')

    # res_compare = read_shp[['osm_id', 'z_order']] == df_dict[['osm_id', 'z_order']]
    # res_compare[res_compare.z_order == False]
    
    gdf_lines = gdf_lines.merge(df_dict, on=['osm_id'], how='left')
    gdf_lines = gdf_lines[['osm_id', 'name', 'highway', 'waterway', 'aerialway', 
                           'barrier', 'man_made', 'z_order', 'other_tags', 'geometry']].reset_index(drop=True)
    return gdf_lines
#

if 'z_order' not in list(gdf_lines.columns):
    gdf_lines = CreateZorderColumn(gdf_lines)
#

##########################
# getting polygon geometry
try:
#     api = overpass.API()
#     poly_resp = api.get(
#         """[out:json][timeout:25];
#         relation({});
#         (._;>;);
#         out geom;""".format(poly_osmid), 
#         build=False, responseformat="json")
#     # Collect coords into list
    coords = []
    for element in poly_resp['elements']:
        if element['type'] == 'way':
            one_line = []
            for j in range(len(element['geometry'])):
                lon = element['geometry'][j]['lon']
                lat = element['geometry'][j]['lat']
                one_line.append((lon, lat))
            coords.append(LineString(one_line))
    #
    ml = MultiLineString(coords)
    buff_ml = ml.buffer(0.000000003)
    pl = ml.convex_hull
    diff = pl.difference(buff_ml)
    lst_len = []
    i=0
    max_ind = -1
    for i in range(len(diff)):
        len_poly = diff[i].length
        lst_len.append(len_poly)
        if len_poly == max(lst_len):
            max_ind = i
    # 
    boundary = diff[max_ind]
    gdf_poly=gpd.GeoDataFrame(geometry=[boundary])
    gdf_poly['osm_id'] = poly_osmid
    gdf_poly.crs='epsg:4326'
except:
    pass
#
if int(buff_km) > 0:
    buffer = int(buff_km) * 1000
    try:
        gdf_poly.geometry = gdf_poly.geometry.to_crs('epsg:32637').buffer(buffer).to_crs('epsg:4326')
    except:
        pass
#



########################
# creating restriction df
def CreateRestr(lst_one,gdf_lines):
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

    gdf_restr = final_df_restr.merge(gdf_lines[['osm_id','geometry']], how='left', on=['osm_id'])
    gdf_restr = gpd.GeoDataFrame(gdf_restr,geometry='geometry')
    gdf_restr.crs='epsg:4326'
    gdf_restr = gdf_restr[~gdf_restr.geometry.isna()].reset_index(drop=True)

    grp_fr = gdf_restr.groupby('osm_id_restr').count()['osm_id']
    grp_fr = grp_fr.reset_index()
    # if one - others arent in graph, should delete
    grp_fr = grp_fr[grp_fr.osm_id < 2]

    gdf_restr = gdf_restr[~gdf_restr.osm_id_restr.isin(grp_fr.osm_id_restr)].reset_index(drop=True)


    np_gdfr = gdf_restr.to_numpy()
    ind_oir = list(gdf_restr.columns).index('osm_id_restr')
    ind_geo = list(gdf_restr.columns).index('geometry')

    lst_via_geo = []
    i=0
    for i in range(1,(len(np_gdfr)-1)):
        
        if np_gdfr[i,ind_oir] == np_gdfr[i-1,ind_oir]:
            elem_1 = i-1
        else:
            elem_1 = i+1
        if np_gdfr[i,ind_geo].coords[0] == np_gdfr[elem_1,ind_geo].coords[0]:
            p_via = Point(np_gdfr[i,ind_geo].coords[0])
        elif np_gdfr[i,ind_geo].coords[-1] == np_gdfr[elem_1,ind_geo].coords[-1]:
            p_via = Point(np_gdfr[i,ind_geo].coords[-1])
        elif np_gdfr[i,ind_geo].coords[-1] == np_gdfr[elem_1,ind_geo].coords[0]:
            p_via = Point(np_gdfr[i,ind_geo].coords[-1])
        elif np_gdfr[i,ind_geo].coords[0] == np_gdfr[elem_1,ind_geo].coords[-1]:
            p_via = Point(np_gdfr[i,ind_geo].coords[0])
        else:
            p_via = "Error"
        lst_via_geo.append(p_via)
    # 
    lst_via_geo.append(lst_via_geo[-1])
    lst_via_geo.insert(0, lst_via_geo[0])


    # tmp_copy = gdf_restr.copy()
    gdf_restr['p_via'] = lst_via_geo
    lst_error = list(gdf_restr[(gdf_restr.p_via == "Error")].osm_id_restr.unique())
    gdf_restr = gdf_restr[~gdf_restr.osm_id_restr.isin(lst_error)].reset_index(drop=True)
    other_lines = gdf_restr.copy()
    other_lines.crs='epsg:4326'
    other_lines['p_via'] = other_lines['p_via'].astype(str)

    other_points = gdf_restr.copy()
    other_points.crs='epsg:4326'
    other_points = other_points.rename(columns={'geometry':'geo_line','p_via':'geometry'})
    other_points['geo_line'] = other_points['geo_line'].astype(str)
    
    return other_points,other_lines

#

#########################
#saving to shp

try:
    other_points,other_lines = CreateRestr(lst_one,gdf_lines)
    other_lines.to_file('{}\\other_lines_{}_{}_{}.shp'.format(path_raw_shp_layers,buff_km, place, str_date), encoding='utf-8')
    other_points.to_file('{}\\other_points_{}_{}_{}.shp'.format(path_raw_shp_layers,buff_km, place, str_date), encoding='utf-8')
except:
    pass
#
# if len(other_others) > 0:
    # other_others.to_file('./data/raw/shp/layers/other_others_{}_{}_{}.shp'.format(buff_km, place, str_date), encoding='utf-8')
#

gdf_lines[['osm_id', 'name', 'highway', 'waterway', 'aerialway',
           'barrier', 'man_made', 'z_order', 
           'other_tags']].to_csv("{}\\csv_{}_{}_{}.csv".format(path_raw_csv,buff_km, place, str_date), sep=";", encoding="utf-8-sig", index=False)
#
# сохранение без геометрии - нужны только поля.
# если кодировка просто utf-8, то сохраняются каракули вместо букв, 
# а windows-1251 или cp1251 не декодируют какие-то конкретные символы, я хз почему

# обрезать для сохранения в шейп
gdf_lines_shp = gdf_lines.copy()

i=0
lst_ot =[]
for i in range(len(gdf_lines_shp)):
    if len(str(gdf_lines_shp.other_tags[i])) > 254:
        str_ot = gdf_lines_shp.other_tags[i][:254]
    else:
        str_ot = gdf_lines_shp.other_tags[i]
    lst_ot.append(str_ot)
# 
gdf_lines_shp['other_tags'] = lst_ot

gdf_lines_shp.to_file("{}\\gdf_lines_{}_{}_{}.shp".format(path_raw_shp_layers,buff_km, place, str_date), encoding="utf-8")
# gdf_points.to_file("{}\\gdf_points_{}_{}_{}.shp".format(path_raw_shp_layers,buff_km, place, str_date), encoding="utf-8")
# gdf_multilines.to_file("{}\\gdf_multilines_{}_{}_{}.shp".format(path_raw_shp_layers,buff_km, place, str_date), encoding="utf-8")
# gdf_multipolygons.to_file("{}\\gdf_multipolygons_{}_{}_{}.shp".format(path_raw_shp_layers,buff_km, place, str_date), encoding="utf-8")
try:
    gdf_poly.to_file('{}\\poly_{}_{}_{}.shp'.format(path_raw_shp_poly,buff_km, place, str_date), encoding='utf-8')
except:
    pass
    print("Borders of the region are not saved to shp")
print("All raw data is saved to folder 'raw'")
#
print("Done")
