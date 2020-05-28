#############################
import parse_osm
#############################
print()
print("___________________________")
print("Part 2. Read osm file and save layers to gdfs_shps.")
print("Please wait...")
print()
import networkx as nx
import osmnx as ox
import pandas as pd
import geopandas as gpd
import shapely
import shapely.wkt
from shapely.geometry import Point, LineString
from tqdm.notebook import tqdm

import wget
from osgeo import gdal, ogr
import os
import momepy
from datetime import datetime
import re
import girs
from girs.feat.layers import LayersReader
import os.path

#############################

# filename = './data/map_2_Yaroslavl,Russia_20200430_2004.osm'
#filename = ''
filename = parse_osm.filename
poly_osmid = parse_osm.poly_osmid
while (os.path.isfile(filename) != True):
    print("Введите путь и название файла OSM, пример:")
    print("./data/map_2_Yaroslavl,Russia_20200430_2004.osm")
    filename = input()
    if os.path.isfile(filename) == False:
        print("Файл не найден, попробуйте снова")
    else:
        print("Файл найден,", filename)
# 

list_split = filename[:-4].rsplit("_")
str_date = list_split[-2] + "_" + list_split[-1]
place = list_split[-3]
buff_km = list_split[-4]

# str_date = '20200430_2004'
# place = 'Yaroslavl,Russia'
# buff_km = '2'

#gdf_poly = parse_osm.gdf_poly
#buff_km = parse_osm.buff_km
#############################
path_data = '.\\data\\' + str_date
path_raw = path_data + '\\raw'
path_raw_csv = path_raw + '\\csv'
path_raw_shp = path_raw + '\\shp'
path_raw_shp_layers = path_raw_shp + '\\layers'
path_raw_shp_poly = path_raw_shp + '\\poly'

#############################
#gdf_poly = ox.gdf_from_place(place, which_result=2, buffer_dist=int(buff_km)*1000)
#gdf_poly.crs='epsg:4326'
#gdf_poly = parse_osm.gdf_poly
#############################

# https://github.com/OSGeo/gdal/blob/master/gdal/data/osmconf.ini
# перед запуском сохрани в папку с этим скриптом файл osmconf.ini из ссылки выше

lrs = LayersReader(filename)

def GirsGdf(lr_nm):
    new_df = lrs.get_geometries_and_field_values(layer_number=lr_nm, geometry_format='wkt')
    lst_geo=[]
    new_df = new_df.reset_index()
    del new_df['FID']
    
    for i in (range(len(new_df))):
        one_geo = new_df._GEOM_[i]
        lst_geo.append(shapely.wkt.loads(one_geo))
    #
    try:
        new_df['geometry'] = lst_geo
        del new_df['_GEOM_']
        new_gdf = gpd.GeoDataFrame(new_df)
        new_gdf.crs='epsg:4326'
    except:
        pass
    del new_df
    return new_gdf
#
try:
    gdf_lines = GirsGdf(1)
    gdf_points = GirsGdf(0)
    gdf_multilines = GirsGdf(2)
    gdf_multipolygons = GirsGdf(3)
except(RuntimeError):
    print("RuntimeError, data is too big")
    exit()
#gdf_other = GirsGdf(4)
del lrs

#extract polygon of the city from all polygons
gdf_poly = gdf_multipolygons[gdf_multipolygons.osm_id==poly_osmid][['osm_id', 'name', 
                                                              'place', 'other_tags', 'geometry']].reset_index(drop=True)
gdf_poly.geometry[0] = gdf_poly.geometry[0][0]
if int(buff_km) > 0:
    buffer = int(buff_km) * 1000
    gdf_poly.geometry = gdf_poly.geometry.to_crs('epsg:32637').buffer(buffer).to_crs('epsg:4326')


gdf_lines = gdf_lines[['osm_id', 'name', 'highway', 'waterway', 'aerialway', 
                           'barrier', 'man_made', 'other_tags', 'geometry']]
                           
# создание поля z_order, алгоритм на sql написан в файле osmconf.ini
# это поле показывает порядок (уровень) ребер/улиц (важно на многоуровневых эстакадах)

# при импорте скрипта для чтения с ОСМ - колонка z_order генерится сама
# без импорта - ниже функция создания этого поля
def CreateZorderColumn(gdf_lines):
    reg = re.compile('[^0-9-]') # only ints
    dict_z_order = {}
    i=0
    for i in range(len(gdf_lines)):
        osmid = gdf_lines.osm_id[i]
        dict_z_order[osmid] = 0
        if gdf_lines.highway[i] in ['minor','road','unclassified','residential']:
            dict_z_order[osmid] = 3
        elif gdf_lines.highway[i] in ['tertiary_link','tertiary']:
            dict_z_order[osmid] = 4
        elif gdf_lines.highway[i] in ['secondary_link','secondary']:
            dict_z_order[osmid] = 6
        elif gdf_lines.highway[i] in ['primary_link','primary']:
            dict_z_order[osmid] = 7
        elif gdf_lines.highway[i] in ['trunk_link','trunk']:
            dict_z_order[osmid] = 8
        elif gdf_lines.highway[i] in ['motorway_link','motorway']:
            dict_z_order[osmid] = 9
        else:
            dict_z_order[osmid] = 0
        #
        if (('"bridge"=>"yes"' in str(gdf_lines['other_tags'][i]))
        or
        ('"bridge"=>"true"' in str(gdf_lines['other_tags'][i]))
        or
        ('"bridge"=>"1"' in str(gdf_lines['other_tags'][i]))):
            dict_z_order[osmid] = dict_z_order[osmid] + 10
        #
        if (('"tunnel"=>"yes"' in str(gdf_lines['other_tags'][i]))
        or
        ('"tunnel"=>"true"' in str(gdf_lines['other_tags'][i]))
        or
        ('"tunnel"=>"1"' in str(gdf_lines['other_tags'][i]))):
            dict_z_order[osmid] = dict_z_order[osmid] - 10
        #
        if ('railway' in str(gdf_lines['other_tags'][i])):
            dict_z_order[osmid] = dict_z_order[osmid] + 5
        #
        if ('layer' in str(gdf_lines['other_tags'][i])):

            str1 = gdf_lines.other_tags[i]
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
gdf_lines = CreateZorderColumn(gdf_lines)


######################

# функция разделения геометрий по типам для слоя other_relations
# сначала разбиение коллекции, а затем сортировка по типам
# (линии отдельно от точек)
######################
final_df_restr = parse_osm.final_df_restr
######################

gdf_restr = final_df_restr.merge(gdf_lines[['osm_id','geometry']], how='left', on=['osm_id'])
gdf_restr = gpd.GeoDataFrame(gdf_restr,geometry='geometry')
gdf_restr.crs='epsg:4326'
gdf_restr = gdf_restr[~gdf_restr.geometry.isna()].reset_index(drop=True)

grp_fr = gdf_restr.groupby('osm_id_restr').count()['osm_id']
grp_fr = grp_fr.reset_index()
# if one - others arent in graph, should delete
grp_fr = grp_fr[grp_fr.osm_id < 2]

gdf_restr = gdf_restr[~gdf_restr.osm_id_restr.isin(grp_fr.osm_id_restr)].reset_index(drop=True)



lst_via_geo = []
i=0
for i in range(1,(len(gdf_restr)-1)):
    
    if gdf_restr.osm_id_restr[i] == gdf_restr.osm_id_restr[i-1]:
        elem_1 = i-1
    else:
        elem_1 = i+1
    if gdf_restr.geometry[i].coords[0] == gdf_restr.geometry[elem_1].coords[0]:
        p_via = Point(gdf_restr.geometry[i].coords[0])
    elif gdf_restr.geometry[i].coords[-1] == gdf_restr.geometry[elem_1].coords[-1]:
        p_via = Point(gdf_restr.geometry[i].coords[-1])
    elif gdf_restr.geometry[i].coords[-1] == gdf_restr.geometry[elem_1].coords[0]:
        p_via = Point(gdf_restr.geometry[i].coords[-1])
    elif gdf_restr.geometry[i].coords[0] == gdf_restr.geometry[elem_1].coords[-1]:
        p_via = Point(gdf_restr.geometry[i].coords[0])
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

# def SepGeomOther (new_gdf):
    # one_osmid = []
    # one_geo = []
    # i=0
    # j=0
    # for i in range(len(new_gdf)):
        # for j in range(len(new_gdf.geometry[i])):
            # one_geo.append(new_gdf.geometry[i][j]) # разбиение коллекции
            # one_osmid.append(new_gdf.osm_id[i])
    # # 
    # all_sep_geo = gpd.GeoDataFrame(columns=['osm_id'], data=one_osmid, geometry=one_geo)
    # all_sep_geo = all_sep_geo.merge(new_gdf[['osm_id', 'name', 'type', 'other_tags']], on=['osm_id'], how='left')
    
    # points = gpd.GeoDataFrame()
    # lines = gpd.GeoDataFrame()
    # others = gpd.GeoDataFrame()
    # i=0
    # for i in range(len(all_sep_geo)):
        # if (isinstance(shapely.geometry.linestring.LineString(), type(all_sep_geo.geometry[i]))):
            # lines = lines.append(all_sep_geo[i:i+1])
        # # 
        # elif (isinstance(shapely.geometry.point.Point(), type(all_sep_geo.geometry[i]))):
            # points = points.append(all_sep_geo[i:i+1])
        # # 
        # else:
            # others = others.append(all_sep_geo[i:i+1])
    # # 
    # lines = lines.reset_index(drop=True)
    # points = points.reset_index(drop=True)
    # others = others.reset_index(drop=True)
    
    # return lines, points, others
# #

# gdf_other_turn = gdf_other[gdf_other['type'] == 'restriction'].copy().reset_index(drop=True)
# other_lines, other_points, other_others = SepGeomOther(gdf_other_turn)

###############################
# saving shp

# print("Do you want to save gdfs to shps?y/n")
# answ_save = input()
# if answ_save == 'y':

other_lines.to_file('{}\\other_lines_{}_{}_{}.shp'.format(path_raw_shp_layers,buff_km, place, str_date), encoding='utf-8')
other_points.to_file('{}\\other_points_{}_{}_{}.shp'.format(path_raw_shp_layers,buff_km, place, str_date), encoding='utf-8')
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

for i in range(len(gdf_lines_shp)):
    if len(str(gdf_lines_shp.other_tags[i])) > 254:
        gdf_lines_shp.other_tags[i] = gdf_lines_shp.other_tags[i][:254]

gdf_lines_shp.to_file("{}\\gdf_lines_{}_{}_{}.shp".format(path_raw_shp_layers,buff_km, place, str_date), encoding="utf-8")
gdf_points.to_file("{}\\gdf_points_{}_{}_{}.shp".format(path_raw_shp_layers,buff_km, place, str_date), encoding="utf-8")
gdf_multilines.to_file("{}\\gdf_multilines_{}_{}_{}.shp".format(path_raw_shp_layers,buff_km, place, str_date), encoding="utf-8")
gdf_multipolygons.to_file("{}\\gdf_multipolygons_{}_{}_{}.shp".format(path_raw_shp_layers,buff_km, place, str_date), encoding="utf-8")
gdf_poly.to_file('{}\\poly_{}_{}_{}.shp'.format(path_raw_shp_poly,buff_km, place, str_date), encoding='utf-8')
print("All raw shps are saved")
#
print("Done")
