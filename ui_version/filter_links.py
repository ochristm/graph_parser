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

#from tqdm.notebook import tqdm

# # В случе ошибки RuntimeError: b'no arguments in initialization list'
# # Если действие выше не помогло, то нужно задать системной переменной PROJ_LIB
# # явный путь к окружению по аналогии ниже
# Для настройки проекции координат, поменять на свой вариант


# os.environ ['PROJ_LIB']=r'C:\Users\popova_kv\AppData\Local\Continuum\anaconda3\Library\share'
# os.environ ['GDAL_DATA']=r'C:\Users\popova_kv\AppData\Local\Continuum\anaconda3\Library\share\gdal'


from shapely import wkt
from shapely.wkt import loads
from shapely.geometry import Point, LineString, MultiLineString, mapping, shape
from shapely.ops import unary_union

import networkx as nx
import momepy
#import osmnx as ox
import re
import math

#отключить предупреждения pandas (так быстрее считает!!!):
pd.options.mode.chained_assignment = None

import warnings
warnings.filterwarnings("ignore")
from time import sleep
pause = 0.1
sleep(pause)
#######################

gdf_lines = gdf_from_osm.gdf_lines
#gdf_multilines = gdf_from_osm.gdf_multilines
str_date = gdf_from_osm.str_date
place = gdf_from_osm.place
buff_km = gdf_from_osm.buff_km
poly_osmid = gdf_from_osm.poly_osmid
# BORDERS!!!!!
gdf_poly = gdf_from_osm.gdf_poly
# gdf_poly = ox.gdf_from_place(place, which_result=2, buffer_dist=int(buff_km)*1000)
# gdf_poly.crs='epsg:4326'

len_gdf = len(gdf_lines)
pause = round(0.000007 * len_gdf, 1)

###############
#path_data = './data/'
#path_res = './data/'

path_data = '.\\data\\' + str(poly_osmid) + '\\' + str_date
path_res = path_data + '\\res'

path_res_edges = path_res
path_res_nodes = path_res
####################

###############
len_elem = len(gdf_lines)
time_min = int(len_elem / 125 / 60)
time_max = int(len_elem / 55 / 60)

time_start = "{:%H:%M:%S}".format(datetime.now())
print("time start:", time_start)
print("Estimated time: {} to {} minutes".format(time_min,time_max))
###############
sleep(pause)


def main(gdf_lines, gdf_poly):


		
	# file_name = 'petr_kamch'
	# read_shp = gpd.read_file(r'./shp/raw/{}.shp'.format(file_name), encoding = 'utf-8', errors='ignore')
	sleep(pause)


	# удаление кривых символов в строке (которые не преобразовались по unicode)
	# city_graph = read_shp.copy()
	city_graph = gdf_lines.copy()
	sleep(pause)
	#################
	#list_columns = [city_graph['name'], city_graph['other_tags']]
	#np_city_gr = city_graph.to_numpy()

	def bytesDecode(list_columns):
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
		return list_new_columns
		# 
	# 
	# ind_name = list(city_graph.columns).index('name')
	# ind_ot = list(city_graph.columns).index('other_tags')
	list_columns = [city_graph['name'], city_graph['other_tags']]
	#list_columns = list(np_city_gr[:,ind_name], np_city_gr[:,ind_ot])

	list_new_columns = bytesDecode(list_columns)
	sleep(pause)


	#################
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
	sleep(pause)
	########
	# https://automating-gis-processes.github.io/site/notebooks/L3/spatial_index.html

	def intersect_using_spatial_index(source_gdf, intersecting_gdf):
		"""
		Conduct spatial intersection using spatial index for candidates GeoDataFrame to make queries faster.
		Note, with this function, you can have multiple Polygons in the 'intersecting_gdf' and it will return all the points
		intersect with ANY of those geometries.
		"""
		source_sindex = source_gdf.sindex
		possible_matches_index = []

		# 'itertuples()' function is a faster version of 'iterrows()'
		for other in intersecting_gdf.itertuples():
			bounds = other.geometry.bounds
			c = list(source_sindex.intersection(bounds))
			possible_matches_index += c

		# Get unique candidates
		unique_candidate_matches = list(set(possible_matches_index))
		possible_matches = source_gdf.iloc[unique_candidate_matches]

		# Conduct the actual intersect
		result = possible_matches.loc[possible_matches.intersects(intersecting_gdf.unary_union)]
		return result
	########
	
		# оставить только те ребра, которые внутри полигона
	try:
		gdf_lines_tmp = intersect_using_spatial_index(city_graph, gdf_poly[['geometry']])
		gdf_lines_tmp = gdf_lines_tmp.reset_index(drop=True)
		# gdf_lines_tmp = gpd.sjoin(gdf_lines, gdf_poly[['geometry']], how='inner', 
							  # op='intersects').drop("index_right", axis=1).reset_index(drop=True)
		#
		if len(gdf_lines_tmp) > (len(city_graph) / 2):
			city_graph = gdf_lines_tmp.copy()
		gdf_lines_tmp = None
		gdf_poly = None
		del gdf_lines_tmp, gdf_poly
	except:
		pass
	#
	
	#########
	#city_graph2 = city_graph.copy()
	cg_crs = city_graph.crs
	np_cg2 = city_graph.to_numpy()
	ind_hw = list(city_graph.columns).index('highway')
	ind_oi = list(city_graph.columns).index('osm_id')

	lst_contstr_name=list(constr.name.unique())
	i=0
	for i in (range(len(lst_contstr_name))):
		one_name = lst_contstr_name[i]
		df_small = constr[constr.name == one_name].reset_index(drop=True)
		sj_df = intersect_using_spatial_index(city_graph,df_small)
	#	 sj_df = gpd.sjoin(df_small, city_graph[['highway', 'name', 'geometry']], 
	#					   how='inner', op='intersects').drop("index_right", axis=1)
		lst_hw = list(sj_df[((sj_df.name == one_name) 
							 & (sj_df.highway != 'construction') 
							 & ((sj_df.highway.astype(str) != 'None')))].highway.unique())
		if lst_hw:
			new_hw = lst_hw[0]
		else:
			new_hw = constr.highway[i]
		for j in range(len(df_small)):
			ind_big = list(np_cg2[:,ind_oi]).index(df_small.osm_id[j])
			np_cg2[ind_big,ind_hw] = new_hw
	# 
	sleep(pause)
	
	lst_col = list(city_graph.columns)

	city_graph = gpd.GeoDataFrame(np_cg2, columns=lst_col)
	city_graph.crs = cg_crs
	#########
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

	sleep(pause)

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
	#####
	#! ATTENTION!!!
	gdf_lines = None
	del gdf_lines
	sleep(pause)
	#####
	########################
	# изменение направления ребра для oneway=-1
	reverse_oneway = city_graph[city_graph.other_tags.str.contains('oneway"=>"-1', 
																   na=False)].reset_index(drop=True)
	#
	####################
	np_city_gr = city_graph.to_numpy()
	lst_rev_on = list(reverse_oneway.osm_id)
	ind_geo = list(city_graph.columns).index('geometry')
	ind_oi = list(city_graph.columns).index('osm_id')

	def RevOnwGeo(np_city_gr,lst_rev_on,ind_geo,ind_oi):
		lst_geo_new=[]
		i=0
		for i in range(len(np_city_gr)):
			if np_city_gr[i][ind_oi] in lst_rev_on:
				list_geo = list(np_city_gr[i][ind_geo].coords[:])
				one_reversed_geo = list_geo[::-1]
				line_2 = LineString(one_reversed_geo)
				lst_geo_new.append(line_2)
			else:
				lst_geo_new.append(np_city_gr[i][ind_geo])
		#
		return lst_geo_new
	# 

	lst_geo_new = RevOnwGeo(np_city_gr,lst_rev_on,ind_geo,ind_oi)
	#################### 
	sleep(pause)

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

	sleep(pause)

	# подтягивание полей с информацией по пересечению геометрий
	graph_info = gpd.sjoin(res_graph, city_graph, how='left', 
							   op='within').drop("index_right", axis=1).reset_index(drop=True)
	#
	#################################
	nans_g = graph_info[graph_info.osm_id.isna()]
	if len (nans_g) != 0:
		nans_g = gpd.sjoin(nans_g[['geometry']], city_graph, how='inner', 
								   op='intersects').drop("index_right", axis=1).reset_index(drop=True)
	#
	sleep(pause)

	# эти необходимо пере_разбить
	tmp_city = city_graph[city_graph.osm_id.isin(nans_g.osm_id)].reset_index(drop=True)

	# удалить пустые и те, которые необходимо пере_разбить, остальные - ок
	good_graph_info = graph_info[((~graph_info.osm_id.isna()) 
								  & (~graph_info.osm_id.isin(tmp_city.osm_id)))].reset_index(drop=True)
	#
	##############
	np_tc = tmp_city.to_numpy()
	ind_geo = list(tmp_city.columns).index('geometry')
	ind_oi = list(tmp_city.columns).index('osm_id')
	
	sleep(pause)
	########


	lst_uu_geo = []
	newlst=[]
	i=0
	for i in (range(len(np_tc))):
		one_geo = gpd.GeoDataFrame(geometry=[np_tc[i,-1]])
		one_geo.crs = city_graph.crs
		tmp_sj = intersect_using_spatial_index(city_graph,one_geo)
	#	 tmp_sj = gpd.sjoin(one_geo, city_graph, how='inner', 
	#						op='intersects').drop("index_right", axis=1).reset_index(drop=True)
		sj_one = list(city_graph[city_graph.osm_id.isin(tmp_sj.osm_id)].geometry)
		lst_one_geo = np_tc[i][ind_geo].coords[:]
		uniqlines = []
		lst_sj_ends=[]
		j=0
		for j in range(len(sj_one)):
			lst_uu = []
			tmp_lst = []
			tmp_lst.append(sj_one[j])
			lst_sj_one_geo = sj_one[j].coords[:]
			res = list(set(lst_one_geo) & set(lst_sj_one_geo)) #find mutual points
			if len(res) > 0:
				for k in res:
					if ((k != np_tc[i][ind_geo].coords[0]) & (k != np_tc[i][ind_geo].coords[-1])):
						if sj_one[j] not in lst_uu:
							lst_uu.append(sj_one[j])
					if ((k == sj_one[j].coords[0]) | (k == sj_one[j].coords[-1])):
						if sj_one[j] not in lst_sj_ends:
							lst_sj_ends.append(sj_one[j])
			#
			for line in lst_uu:
				if not any(p.equals(line) for p in uniqlines):
					uniqlines.append(line)
		if np_tc[i][ind_geo] not in uniqlines:
				uniqlines.append(np_tc[i][ind_geo])
		if len(uniqlines) > 1:
			uu_geo = unary_union(uniqlines)
			one_gdf = gpd.GeoDataFrame(geometry=list(uu_geo))
			one_gdf.crs=city_graph.crs
			tmp_one_gdf = gpd.sjoin(one_gdf, city_graph, how='left', 
									op='within').drop("index_right", axis=1)
			if len(tmp_one_gdf[tmp_one_gdf.osm_id.isna()]) > 0:
				line_f = np_tc[i][ind_geo]
				line = np_tc[i][ind_geo]
				d=0
				cnt=0
				for d in range(len(lst_sj_ends)):
					try:
						point = Point(list(set(line_f.coords[:]) & set(lst_sj_ends[d].coords[:]))[0])
						if (((point.coords[0] != line_f.coords[0]) & (point.coords[0] != line_f.coords[-1])) 
							& ((point.coords[0] == lst_sj_ends[d].coords[0]) | (point.coords[0] == lst_sj_ends[d].coords[-1]))):
							new_geo = MultiLineString(list(shapely.ops.split(line,point)))
							line = new_geo
							cnt+=1
					except:
						pass
				if cnt > 0:
					newlst.append(new_geo)
				else:
					new_geo = np_tc[i][ind_geo]
			else:
				new_geo = MultiLineString(list(tmp_one_gdf[tmp_one_gdf.osm_id 
																		== np_tc[i][ind_oi]].geometry))
		else:
			new_geo = np_tc[i][ind_geo]
		#
		lst_uu_geo.append(new_geo)
	# 
	####
	# ! ATTENTION
	city_graph = None
	del city_graph
	####
	
	sleep(pause)
	##############
	#
	try:
		tmp_city['uu_geo'] = lst_uu_geo
	except:
		print("Error_uu")
	#
	###############
	np_tmp_ct = tmp_city.to_numpy()
	ind_uug = list(tmp_city.columns).index('uu_geo')
	
	sleep(pause)

	new_df = []
	i=0
	for i in (range(len(np_tmp_ct))):
		
		one_line = np_tmp_ct[i][ind_uug]
		try:
			len_ol = len(one_line)
			j = 0
			for j in range(len_ol):
				lst_one = list(np_tmp_ct[i][:ind_uug])
				lst_one.append(one_line[j])
				new_df.append(lst_one)
		except:
			new_df.append(list(np_tmp_ct[i]))
	# 
	
	sleep(pause)
	
	###############
	new_gdf = gpd.GeoDataFrame(columns=tmp_city.columns, data=new_df)
	del new_gdf['geometry']
	new_gdf = new_gdf.rename(columns={'uu_geo':'geometry'})
	new_gdf.crs='epsg:4326'

	tr_gi = good_graph_info.append(new_gdf).reset_index(drop=True)
	graph_filtrd = tr_gi.copy()
	graph_filtrd['z_order'] = graph_filtrd['z_order'].astype(np.int64)
	#################################
	
	sleep(pause)

	#################################################
	# создание digraph (двунаправленного графа)

	###### Create Reverse of graph
	#direct_gdf = new_graph[['osm_id', 'name', 'highway', 'z_order', 'other_tags', 'geometry']].copy()

	direct_gdf = graph_filtrd[['osm_id', 'name', 'highway', 'z_order', 'other_tags', 'geometry']].copy()
	reverse_gdf = direct_gdf.copy()
	
	sleep(pause)

	###############
	np_rev_gdf = reverse_gdf.to_numpy()
	ind_geo = list(reverse_gdf.columns).index('geometry')
	reversed_geo_all = []
	i=0
	for i in (range(len(np_rev_gdf))):
		list_geo = list(np_rev_gdf[i][ind_geo].coords[:])
		one_reversed_geo = list_geo[::-1]
		line_2 = LineString(one_reversed_geo)
		reversed_geo_all.append(line_2)
	# 
	###############

	#rev_geo = gpd.GeoDataFrame(geometry=reversed_geo_all)
	try:
		reverse_gdf['rev_geo'] = reversed_geo_all
		reverse_gdf = reverse_gdf.rename(columns={'geometry':'old_geo', 'rev_geo':'geometry'})
		reverse_gdf = reverse_gdf[['osm_id', 'name', 'highway', 'z_order', 'other_tags', 'geometry']]
	except:
		print("Error!")
	#
	sleep(pause)


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

	#############
	np_tmp_gr = tmp_grph.to_numpy()
	ind_geo = list(tmp_grph.columns).index('geometry')
	
	sleep(pause)

	i=0
	for i in (range(len(np_tmp_gr))):
		if np_tmp_gr[i][ind_geo].coords[0] == np_tmp_gr[i][ind_geo].coords[-1]:
			lst_petl.append(np_tmp_gr[i][ind_geo])
		else:
			lst_stend.append(str(np_tmp_gr[i][ind_geo].coords[0]) + "_" + str(np_tmp_gr[i][ind_geo].coords[-1]))
	# 
	#############
	sleep(pause)

	my_dict = {i:lst_stend.count(i) for i in lst_stend}

	newDict = {}
	for (key, value) in my_dict.items():
		if value > 1:
			newDict[key] = value
	#

	#
	lst_geo = []
	lst_len_geo = []
	###########
	# np_tmp_gr = tmp_grph.to_numpy()
	# ind_geo = list(tmp_grph.columns).index('geometry')
	i=0
	for i in range(len(np_tmp_gr)):
		str_st_end = str(np_tmp_gr[i][ind_geo].coords[0]) + "_" + str(np_tmp_gr[i][ind_geo].coords[-1])
		if str_st_end in (newDict.keys()):
			lst_geo.append(str_st_end)
			lst_len_geo.append(np_tmp_gr[i][ind_geo].length)
		else:
			lst_geo.append(None)
			lst_len_geo.append(None)
	#
	###########
	
	sleep(pause)

	tmp_grph['geo_grup'] = lst_geo
	tmp_grph['geo_len'] = lst_len_geo

	################
	np_tmp_gr = tmp_grph.to_numpy()
	ind_ggr = list(tmp_grph.columns).index('geo_grup')
	ind_glen = list(tmp_grph.columns).index('geo_len')

	dct_gr_len = {}
	i=0
	for i in range(len(np_tmp_gr)):
		one_group = np_tmp_gr[i][ind_ggr]
		if one_group not in dct_gr_len.keys():
			lst_gr_len = []
			dct_gr_len[one_group] = lst_gr_len
		dct_gr_len[one_group] = dct_gr_len[one_group] + [np_tmp_gr[i][ind_glen]]  
	# 
	################
	sleep(pause)

	del dct_gr_len[None]
	del tmp_grph['geo_len'], tmp_grph['geo_grup']

	################
	np_tmp_gr = tmp_grph.to_numpy()
	ind_geo = list(tmp_grph.columns).index('geometry')

	lst_max = []
	i=0
	for i in range(len(np_tmp_gr)):
		one_group = str(np_tmp_gr[i][ind_geo].coords[0]) + "_" + str(np_tmp_gr[i][ind_geo].coords[-1])
		if one_group in dct_gr_len.keys():
			if (max(dct_gr_len[one_group]) == np_tmp_gr[i][ind_geo].length):
				lst_max.append(1)
			else:
				lst_max.append(None)
		elif np_tmp_gr[i][ind_geo].coords[0] == np_tmp_gr[i][ind_geo].coords[-1]:
			lst_max.append(1)
		else:
			lst_max.append(None)
	# 
	################

	try:
		tmp_grph['cut_geo'] = lst_max
	except:
		print("Error_cut_double")
	#
	#########################
	
	sleep(pause)
	
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

	################
	np_ctgdf = cut_gdf.to_numpy()
	ind_geo = list(cut_gdf.columns).index('geometry')

	big_lst = []
	i=0
	for i in (range(len(np_ctgdf))):
		line = np_ctgdf[i][ind_geo]
		lst_one_geo = cut(line, (line.length / 2))
		one_list = list(np_ctgdf[i,:ind_geo]) + [lst_one_geo[0]] + list(np_ctgdf[i,ind_geo+1:])
		two_list = list(np_ctgdf[i,:ind_geo]) + [lst_one_geo[1]] + list(np_ctgdf[i,ind_geo+1:])
		big_lst.append(one_list)
		big_lst.append(two_list)
	# 
	sleep(pause)

	big_gdf = gpd.GeoDataFrame(big_lst, columns=list(cut_gdf.columns))
	################ 
	big_gdf = all_ok.append(big_gdf).reset_index(drop=True)

	del big_gdf['cut_geo']

	big_gdf.crs = 'epsg:4326'
	big_gdf = big_gdf.to_crs('epsg:32637')
	#########################
	
	sleep(pause)
	
	############################################

	##############

	def make_graph_great_again(gdf):
		G = momepy.gdf_to_nx(gdf, approach='primal')
		
		# выбор наибольшего графа из подграфов 
		# (когда ребра удаляются выше, остаются подвешенные куски графа, их надо удалить)
		 # whatever graph you're working with
		cur_graph = G
	#	 def del_subgraphs(cur_graph):
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
			#
		#####
		
		#create gdfs
		# формирование таблиц из графа и узлов (nodes)
		nodes, new_graph = momepy.nx_to_gdf(cur_graph)

		return nodes, new_graph

	all_nodes, all_edges = make_graph_great_again(big_gdf)
	
	sleep(pause)

	#################################################

	# all_graph = momepy.gdf_to_nx(big_gdf)
	# all_nodes, all_edges = momepy.nx_to_gdf(all_graph)

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

	##################
	np_cp = check_points.to_numpy()
	ind_ne = list(check_points.columns).index('node_end')
	ind_ns = list(check_points.columns).index('node_start')
	ind_sg = list(check_points.columns).index('start_geometry')
	ind_eg = list(check_points.columns).index('end_geometry')
	ind_lg = list(check_points.columns).index('line_geometry')

	# check_points['start_true'] = None
	# check_points['end_true'] = None
	
	sleep(pause)

	list_check_start = []
	list_check_end = []
	i=0
	for i in (range(len(np_cp))):
		if np_cp[i][ind_sg].coords[0] == np_cp[i][ind_lg].coords[0]:
			list_check_start.append(np_cp[i][ind_ns])
		elif np_cp[i][ind_eg].coords[0] == np_cp[i][ind_lg].coords[0]:
			list_check_start.append(np_cp[i][ind_ne])
		else:
			list_check_start.append(None)
	# 

	for ii in (range(len(np_cp))):
		if np_cp[ii][ind_eg].coords[0] == np_cp[ii][ind_lg].coords[-1]:
			list_check_end.append(np_cp[ii][ind_ne])
		elif np_cp[ii][ind_sg].coords[0] == np_cp[ii][ind_lg].coords[-1]:
			list_check_end.append(np_cp[ii][ind_ns])
		else:
			list_check_end.append(None)
	#
	##################

	try:
		check_points['start_true'] = list_check_start
		check_points['end_true'] = list_check_end
	except:
		print("Error1")
	# 
	
	sleep(pause)

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
	
	sleep(pause)

	############
	# create num_lanes column
	np_gf = graph_full.to_numpy()
	ind_ot = list(graph_full.columns).index('other_tags')
	ind_dir = list(graph_full.columns).index('direction')

	list_lanes = []
	reg = re.compile('[^0-9]')
	
	sleep(pause)

	lst_onw=['oneway"=>"yes','oneway"=>"1','oneway"=>"true', 'oneway"=>"-1']
	i=0
	for i in range(len(np_gf)):
		str1 = str(np_gf[i][ind_ot])
		if any((c in str1) for c in lst_onw):
			if np_gf[i][ind_dir] == 'direct':
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
					if np_gf[i][ind_dir] == 'direct':
						list_lanes.append(math.ceil(int_lanes/2))
					else:
						list_lanes.append(math.floor(int_lanes/2))
				else:
					list_lanes.append(1)
			else:
				list_lanes.append(1)
	# 
	
	sleep(pause)
	############

	try:
		graph_full['NUMLANES'] = list_lanes
	except:
		print("Error2")
	#  

	#############
	np_gf = graph_full.to_numpy()
	ind_ot = list(graph_full.columns).index('other_tags')
	lst_types = []
	
	sleep(pause)
	
	i=0
	for i in (range(len(np_gf))):
		if "railway" in str(np_gf[i][ind_ot]):
			if '=>"tram"' in str(np_gf[i][ind_ot]):
				if 'surface' in str(np_gf[i][ind_ot]):
					lst_types.append("TM,CAR,BUS,TB,MT")
				else:
					lst_types.append("TM")
			elif 'subway' in str(np_gf[i][ind_ot]):
				lst_types.append("MTR")
			else:
				lst_types.append("E")
		else:
			if (
				('psv"=>"only"' in str(np_gf[i][ind_ot]))
				|
				(('psv"=>"yes"' in str(np_gf[i][ind_ot]))
					& ('vehicle"=>"no"' in str(np_gf[i][ind_ot])))):
				lst_types.append("BUS,TB,MT")
			else:
				lst_types.append("CAR,BUS,TB,MT")
	#
	
	sleep(pause)
	#############

	try:
		graph_full['TSYSSET'] = lst_types
	except:
		print("Error3")
	# 

	##############################
	# add type link

	################
	# add type link
	np_gf = graph_full.to_numpy()
	ind_ot = list(graph_full.columns).index('other_tags')
	ind_hw = list(graph_full.columns).index('highway')
	ind_nm = list(graph_full.columns).index('name')
	ind_nl = list(graph_full.columns).index('NUMLANES')
	
	sleep(pause)

	lst_typeno = []
	i=0
	for i in range(len(np_gf)):
		if "railway" in str(np_gf[i][ind_ot]):
			if "subway" in str(np_gf[i][ind_ot]):
				lst_typeno.append(10)
			elif "tram" in str(np_gf[i][ind_ot]):
				lst_typeno.append(40)
			else:
				lst_typeno.append(20)
		elif np_gf[i][ind_nl] == 0:
			lst_typeno.append(0)
		else:
			if np_gf[i][ind_hw] in ['motorway','trunk']:
				lst_typeno.append(1)
			elif np_gf[i][ind_hw] == 'primary':
				lst_typeno.append(2)
			elif np_gf[i][ind_hw] == 'secondary':
				lst_typeno.append(3)
			elif np_gf[i][ind_hw] == 'tertiary':
				lst_typeno.append(4)
			elif np_gf[i][ind_hw] in ['motorway_link','trunk_link', 'primary_link', 'secondary_link', 'tertiary_link']:
				lst_typeno.append(5)
			elif np_gf[i][ind_nm] != None:
				lst_typeno.append(6)
			else:
				lst_typeno.append(7)
	# 
	
	sleep(pause)
	################

	try:
		graph_full['TYPENO_2'] = lst_typeno
	except:
		print("Error_typeno")

	##############################

	graph_full.crs='epsg:4326'
	graph_full = graph_full.rename(columns={'link_id':'NO', 'mm_len':'LENGTH', 
											'node_start':'FROMNODENO', 'node_end':'TONODENO'})
	#
	return graph_full, all_nodes
#
######### End of graph checking #############

sleep(pause)

graph_full,all_nodes = main(gdf_lines, gdf_poly)

sleep(pause)

#################################################



def saveMe(graph_full,all_nodes,str_date,place,buff_km,poly_osmid):

	graph_full_shp = graph_full.copy()
	np_gf = graph_full_shp.to_numpy()
	ind_ot = list(graph_full_shp.columns).index('other_tags')
	
	sleep(pause)
	
	# обрезать для сохранения в шейп
	lst_ot = []
	i=0
	for i in range(len(np_gf)):
		if len(str(np_gf[i][ind_ot])) > 254:
			lst_ot.append(np_gf[i][ind_ot][:254])
		else:
			lst_ot.append(np_gf[i][ind_ot])
	# 
	graph_full_shp['other_tags'] = lst_ot
	
	sleep(pause)

	graph_full_shp.to_file('{}\\new_graph_{}_{}_{}.shp'.format(path_res_edges,buff_km, place, str_date), encoding='utf-8')
	#graph_full_shp.to_file('./new_graph_{}_{}_{}.shp'.format(buff_km, place, str_date), encoding='utf-8')


	all_nodes = all_nodes.rename(columns={'nodeID':'NO'})
	all_nodes['XCOORD'] = all_nodes.geometry.x
	all_nodes['YCOORD'] = all_nodes.geometry.y
	all_nodes.to_file('{}\\nodes_{}_{}_{}.shp'.format(path_res_nodes,buff_km, place, str_date), encoding='utf-8')
	#all_nodes.to_file('./all_nodes_{}_{}_{}.shp'.format(buff_km, place, str_date), encoding='utf-8')
#
sleep(pause)
saveMe(graph_full,all_nodes,str_date,place,buff_km,poly_osmid)

time_end = "{:%H:%M:%S}".format(datetime.now())
print("time end:", time_end)
print("Results are in folder 'res'")
print("Done")