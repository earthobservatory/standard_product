#!/usr/bin/env python

'''
Returns True/False if there is land in the given point/polygon
'''

from __future__ import print_function
import os
import json
import pyproj
from functools import partial
from shapely.geometry import shape, Polygon, MultiPolygon, mapping
from shapely.ops import cascaded_union
import shapely.ops
import fiona

def covers_land(geojson):
    '''Determines if there is any land over the geojson. Returns True or False'''
    geojson = validate_geojson(geojson)
    #geojson = shape(geojson)
    land_shapes = get_shapes(oftype='land')
    for shapeobj in land_shapes:
        if shapeobj.intersects(geojson):
            return True
    return False

def covers_water(geojson):
    '''Determines if there is any water over the geojson. Returns True or False'''
    geojson = validate_geojson(geojson)
    #geojson = shape(geojson)
    water_shapes = get_shapes(oftype='water')
    for shapeobj in water_shapes:
        if shapeobj.intersects(geojson):
            return True
    return False

def covers_only_land(geojson):
    '''returns True if the geojson only covers land, False if there is any water in the scene'''
    return not covers_water(geojson)

def covers_only_water(geojson):
    '''returns True if the geojson only covers water, False if there is any land in the scene'''
    return not covers_land(geojson)

def get_land_area(geojson):
    '''returns the amount of land covering the geojson in km^2'''
    geojson = validate_geojson(geojson)
    land_shapes = get_shapes(oftype='land')
    intersecting_land_shapes = []
    for shapeobj in land_shapes:
        if shapeobj.intersects(geojson) or geojson.contains(shapeobj):
            if shapeobj.contains(geojson):
                print("RETURNING : %s" %geojson)
                return get_area(geojson)
            intersecting_land_shapes.append(geojson.intersection(shapeobj))
    area = get_area(MultiPolygon(intersecting_land_shapes))
    return area

def get_water_area(geojson):
    '''Returns the amount of water covering the geojson in km^2'''
    geojson = validate_geojson(geojson)
    land = get_land_area(geojson)
    #geojson = shape(geojson)
    #geojson = Polygon(geojson['coordinates'][0])
    return get_area(shape(geojson)) - land

def get_land_percentage(geojson):
    '''Returns the percentage of area covered by land. 0.0 to 1.0'''
    #return get_land_area(geojson) / get_area(Polygon(geojson['coordinates'][0]))
    geojson = validate_geojson(geojson)
    return get_land_area(geojson) / get_area(geojson)

def get_water_percentage(geojson):
    '''Returns the percentage of area covered by water. 0.0 to 1.0'''
    #return get_water_area(geojson) / get_area(Polygon(geojson['coordinates'][0]))
    geojson = validate_geojson(geojson)
    return get_water_area(geojson) / get_area(geojson)

def get_polygons(geojson, oftype='land'):
    '''returns a list of land area polygons that intersect the input geojson, for either land or water, cropped to the input geojson extent'''
    intersecting_shapes = []
    geojson = validate_geojson(geojson)
    landwater_shapes = get_shapes(oftype=oftype)
    for shapeobj in landwater_shapes:
        if shapeobj.intersects(geojson):
            if shapeobj.contains(geojson):
                return mapping(geojson)
            intrsct = geojson.intersection(shapeobj)
            intersecting_shapes.append(intrsct)
    if len(intersecting_shapes) == 0:
        return None
    if len(intersecting_shapes) == 1:
        return mapping(intersecting_shapes[0])
    multi = MultiPolygon(intersecting_shapes)
    return mapping(cascaded_union(multi))

def get_land_polygons(geojson):
    '''returns a list of land area polygons that intersect the input geojson, cropped to the input geojson extent'''
    return get_polygons(geojson, oftype='land')

def get_water_polygons(geojson):
    '''returns a list of water area polygons that intersect the input geojson, cropped to the input geojson extent'''
    return get_polygons(geojson, oftype='water')

def get_area(geojson):
    '''Returns the area of the polygon'''
    geojson = validate_geojson(geojson)
    newshape = shapely.ops.transform(partial(pyproj.transform, pyproj.Proj(init='EPSG:4326'),
                                     pyproj.Proj(proj='aea')), geojson)
    return newshape.area / 10.0**6

def get_shapes(oftype='land'):
    '''loads all the shapes from the water shapefile'''
    shapefile = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data', 'simplified_{0}_polygons.shp'.format(oftype))
    shapes = []
    if not os.path.exists(shapefile):
        raise Exception('Required data file does not exist: {0}'.format(shapefile))
    with fiona.collection(shapefile, 'r') as inp:
        for geom in inp:
            sp = shape(geom['geometry'])
            shapes.append(sp)
    return shapes

def validate_geojson(geojson):
    '''validates the geojson and converts it into a shapely object. can accept strings, shapefiles & geojson dicts'''
    if isinstance(geojson, str):
        geojson = json.loads(geojson)
    if isinstance(geojson, shapely.geometry.polygon.Polygon):
        return geojson
    shp = shape(geojson)
    if shp.is_valid:
        return shp
    else:
        raise Exception('input geojson {0} is not valid'.format(geojson))

def test():
    '''runs a test over sicily, hawaii, etc'''
    test_dict = {}
    test_dict['land_and_water_polygon'] = [[12.891683578491213, 38.37789851200675], [11.765542030334474, 38.11254460084754], [12.869324684143068, 37.133463744616456], [14.642543792724611, 36.54194981843648], [15.757741928100588, 36.6001975253107], [15.576252937316896, 37.30911647598541], [15.412702560424806, 37.67132087507], [15.558614730834961, 38.02473460822767], [15.613503456115724, 38.38180096629129], [14.523024559020998, 38.243202382713605], [13.564982414245607, 38.38566957092227], [12.891683578491213, 38.37789851200675]]
    test_dict['land_only_polygon'] = [[-118.72972011566164, 34.96358310815083], [-118.7426805496216, 34.95392512349466], [-118.741851747036, 34.94128020346737], [-118.71324330568315, 34.94121204335055], [-118.7131454050541, 34.958508603969236], [-118.72972011566164, 34.96358310815083]]
    test_dict['water_only_polygon'] = [[-120.3117620944977, 32.9773526159236], [-120.35270333290102, 32.75546576141111], [-120.1521009206772, 32.67390732403642], [-119.98687148094179, 32.86846786484173], [-120.13354003429414, 33.02966016839023], [-120.3117620944977, 32.9773526159236]]
    test_dict['iceland'] = [[-24.94102478027344,66.62561451469584],[-25.57823181152344,64.39753122058228],[-21.39690399169922,62.66783857582993],[-14.134597778320314,63.92877326933141],[-12.479438781738283,65.8834234934428],[-15.170745849609377,67.10232345139119],[-20.71918487548828,66.8000257591103],[-24.94102478027344,66.62561451469584]]
    test_dict['hawaii'] = [[-179.04796600341797,30.02213803127762],[-179.18907165527344,27.3516430588189],[-154.93640899658206,17.047594180752778],[-152.70618438720703,20.787893513679396],[-176.56642913818362,30.084542946324945],[-179.04796600341797,30.02213803127762]] 
    test_dict['new_zealand'] = [[168.65386962890628,-33.35462041843625],[159.8057556152344,-46.10323266470107],[173.57917785644534,-49.428840000635226],[183.55407714843753,-37.05572508596021],[173.2358551025391,-30.40959743218008],[168.65386962890628,-33.35462041843625]]
    test_dict['caspian'] = [[50.81485748291016,47.31101290750725],[46.719017028808594,45.51151979926975],[46.25965118408204,44.07093712790448],[48.88195037841797,40.34065649361507],[48.49777221679688,37.68219008286376],[51.170883178710945,36.26766697814671],[54.774913787841804,36.636330360763424],[54.51227188110352,40.06296452858627],[53.103275299072266,40.7290474687069],[52.686481475830085,42.31260817230085],[53.21794509887696,43.012806405561534],[51.24092102050782,44.009607826541234],[51.98129653930665,45.02051982382388],[54.285507202148445,46.16746780081259],[53.84605407714844,47.512679047971524],[50.81485748291016,47.31101290750725]]
    for name, coords in test_dict.iteritems():
        geojson = {"type":"Polygon", "coordinates": [coords]}
        print('------------------------------------------------------')
        print('Evaluating area:   {}'.format(name))
        print('Covers any water?  {}'.format(covers_water(geojson)))
        print('Covers any land?   {}'.format(covers_land(geojson)))
        print('Covers only land?  {}'.format(covers_only_land(geojson)))
        print('Covers only water? {}'.format(covers_only_water(geojson)))
        print('Land area:         {:,.2f} km^2'.format(get_land_area(geojson)))
        print('Water area:        {:,.2f} km^2'.format(get_water_area(geojson)))
        print('Land coverage:     {}'.format(get_land_percentage(geojson)))
        print('Water coverage:    {}'.format(get_water_percentage(geojson)))
        #print('Land shapes:       {:<300}'.format(get_land_polygons(geojson)))
        #print('Water shapes:      {:<300}'.format(get_water_polygons(geojson)))

if __name__ == '__main__':
    test()
