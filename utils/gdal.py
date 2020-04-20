import re
import os
import subprocess

def detect_epsg(filename):
    gdal_cmd = ['gdalsrsinfo', '-e', filename]
    gdal = subprocess.Popen(gdal_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    gdal_output = gdal.communicate()

    result = None
    for line in gdal_output[0].split('\n'):
        if line.startswith('EPSG') and line != 'EPSG:-1':
            result = line
            break

    return result

layer_re = re.compile("[\r\n]+Layer name:")
layer_info_re = re.compile("[\r\n]+(?P<key>[a-zA-Z0-9_\-][a-zA-Z0-9_\- ]*)[ \t]*[:=](?P<value>[^\r\n]*([\r\n]+(([ \t]+[^\r\n]*)|(GEOGCS[^\r\n]*)))*)")
extent_re = re.compile("\s*\(\s*(?P<minx>-?[0-9\.]+)\s*\,\s*(?P<miny>-?[0-9\.]+)\s*\)\s*\-\s*\(\s*(?P<maxx>-?[0-9\.]+)\s*\,\s*(?P<maxy>-?[0-9\.]+)\s*\)\s*")
field_re = re.compile("[ \t]*(?P<type>[a-zA-Z0-9]+)[ \t]*(\([ \t]*(?P<width>[0-9]+)\.(?P<precision>[0-9]+)\))?[ \t]*")
def get_layers(datasource,layer=None):
    """
    Get layers' meta data from spatial data file
    layer: only get the specified layer from spatial data file; if none, get all layers 
    Return a list of layer's metadata
       fields: a list of fields
       features: the number of features
       extent: the extent of the layer
       fid_column: the feature id column
       geometry_column: the geometry column
    """
    # needs gdal 1.10+
    infoIter = None

    folder,filename = os.path.split(datasource)
    #import ipdb;ipdb.set_trace()
    cmd = "cd {0} && ogrinfo -al -so -ro {1}".format(folder,filename)

    if layer:
        cmd.append(layer)

    def getLayerInfo(layerInfo):
        info = {"fields":[]}
        for m in layer_info_re.finditer(layerInfo):
            key = m.group("key")
            lkey = key.lower()
            value = m.group("value").strip()
            if lkey in ("info","metadata","layer srs wkt","ogrinfo"): 
                continue
            if lkey == "layer name":
                info["layer"] = value
            elif lkey == "geometry":
                info["geometry"] = value.replace(" ","").upper()
            elif lkey == "feature count":
                try:
                    info["features"] = int(value)
                except:
                    info["features"] = 0
            elif lkey == "extent":
                try:
                    info["extent"] = [float(v) for v in extent_re.search(value).groups()]
                except:
                    pass
            elif lkey == "fid column":
                info["fid_column"] = value
            elif lkey == "geometry column":
                info["geometry_column"] = value
            else:
                m = field_re.search(value)
                info["fields"].append([lkey,m.group('type'),m.group('width'),m.group('precision')])

        return info
    info = subprocess.check_output(cmd,shell=True).decode()
    layers = []
    previousMatch = None
    layerIter = layer_re.finditer(info)
    for m in layerIter:
        if previousMatch is None:
            previousMatch = m
        else:
            layers.append(getLayerInfo(info[previousMatch.start():m.start()]))
            previousMatch = m
    if previousMatch:
        layers.append(getLayerInfo(info[previousMatch.start():]))

    return layers

def get_feature_count(datasource,layer=None):
    """
    Return the feature count of the specified layer or the first layer
    """
    layers = getLayers(datasource,layer)
    if len(layers) == 0:
        raise Exception("Layer({}) is not found in datasource({})".format(layer or "",datasource))
    elif len(layers) > 1:
        raise Exception("Multiple layers are found in datasource({})".format(datasource))
    else:
        return layers[0].get("features") or 0

