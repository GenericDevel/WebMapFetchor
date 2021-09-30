# -*- coding: utf-8 -*
'''
This code is used to download image from google

@date  : 2020-3-13
@author: Zheng Jie
@E-mail: zhengjie9510@qq.com

 Modify by : LiXilin
 email     : ligq168@csu.edu.cn
 date      : 2021/9/30/
'''

import io
import math
import multiprocessing
import time
import urllib.request as ur
from math import fabs, floor, pi, log, tan, atan, exp
from threading import Thread

import PIL.Image as pil
import cv2
import numpy as np
from osgeo import gdal, osr
import Utils


# -----------------------------------------------------------
# multiple threads downloader
class Downloader(Thread):
    ''' Tile Downloader'''
    def __init__(self, index, count, urls, datas):
        # index represents the number of threads
        # count represents the total number of threads
        # urls represents the list of URLs nedd to be downloaded
        # datas represents the list of data need to be returned.
        super().__init__()
        self.urls = urls
        self.datas = datas
        self.index = index
        self.count = count

    def download(self, url):
        HEADERS = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36 Edg/88.0.705.68'}
        header = ur.Request(url, headers=HEADERS)
        err = 0
        while (err < 3):
            try:
                print(" Fetching => ",url);
                data = ur.urlopen(header).read()
            except:
                err += 1
            else:
                return data
        raise Exception("Bad network link.")

    def run(self):
        for i, url in enumerate(self.urls):
            if i % self.count != self.index:
                continue
            self.datas[i] = self.download(url)


# ---------------------------------------------------------
def getExtent(x1, y1, x2, y2, z, source="Google-China"):
    pos1x, pos1y = Utils.wgs_to_tile(x1, y1, z)
    pos2x, pos2y = Utils.wgs_to_tile(x2, y2, z)
    Xframe = Utils.pixls_to_mercator(
        {"LT": (pos1x, pos1y), "RT": (pos2x, pos1y), "LB": (pos1x, pos2y), "RB": (pos2x, pos2y), "z": z})
    for i in ["LT", "LB", "RT", "RB"]:
        Xframe[i] = Utils.mercator_to_wgs(*Xframe[i])
        
    if source != "Google-China":
        pass
    elif source == "Google-China":
        for i in ["LT", "LB", "RT", "RB"]:
            Xframe[i] = Utils.gcj_to_wgs(*Xframe[i])
    else:
        raise Exception("Invalid argument: source.")
    return Xframe


def saveTiff(r, g, b, gt, filePath):
    fname_out = filePath
    driver = gdal.GetDriverByName('GTiff')
    # Create a 3-band dataset
    dset_output = driver.Create(fname_out, r.shape[1], r.shape[0], 3, gdal.GDT_Byte)
    dset_output.SetGeoTransform(gt)
    try:
        proj = osr.SpatialReference()
        proj.ImportFromEPSG(4326)
        dset_output.SetSpatialRef(proj)
    except:
        print("Error: Coordinate system setting failed")
    dset_output.GetRasterBand(1).WriteArray(r)
    dset_output.GetRasterBand(2).WriteArray(g)
    dset_output.GetRasterBand(3).WriteArray(b)
    dset_output.FlushCache()
    dset_output = None
    print("Image Saved")


# ---------------------------------------------------------

# ---------------------------------------------------------
MAP_URLS = {
    "Google": "http://mts0.googleapis.com/vt?lyrs={style}&x={x}&y={y}&z={z}",
    "Google-China": "http://mt2.google.cn/vt/lyrs={style}&hl=zh-CN&gl=CN&src=app&x={x}&y={y}&z={z}",
    "ESRI":"https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
    "Bing":'http://tiles.virtualearth.net/tiles/h{key}.jpeg?g=461&mkt=en-us&n=z',
    "Tianditu":"http://t4.tianditu.gov.cn/DataServer?T=img_w&x={x}&y={y}&l={z}&tk=ef6151d9f0386f3b2a2fdf1d58fe9b32"
    }


def get_url(source, x, y, z, style):  
    '''
    parse the url
    source : the server type
    x : tile x_no
    y : tile y_no
    z : zoom or level
    '''
    # The level(z) maximum value is 18.
    if source=='Tianditu':
        if z>18:
            z=18;
            
    url = MAP_URLS[source].format(x=x, y=y, z=z, style=style);
    if url=="":
        url = MAP_URLS["Google"].format(x=x, y=y, z=z, style=style)
    
    return url


def get_urls(x1, y1, x2, y2, z, source, style):
    pos1x, pos1y = Utils.wgs_to_tile(x1, y1, z)
    pos2x, pos2y = Utils.wgs_to_tile(x2, y2, z)
    lenx = pos2x - pos1x + 1
    leny = pos2y - pos1y + 1
    print("Total tiles number：{x} X {y}".format(x=lenx, y=leny))
    urls = [get_url(source, i, j, z, style) for j in range(pos1y, pos1y + leny) for i in range(pos1x, pos1x + lenx)]
    return urls


# ---------------------------------------------------------

# ---------------------------------------------------------
def merge_tiles(datas, x1, y1, x2, y2, z):
    pos1x, pos1y = Utils.wgs_to_tile(x1, y1, z)
    pos2x, pos2y = Utils.wgs_to_tile(x2, y2, z)
    lenx = pos2x - pos1x + 1
    leny = pos2y - pos1y + 1
    outpic = pil.new('RGBA', (lenx * 256, leny * 256))
    for i, data in enumerate(datas):
        picio = io.BytesIO(data)
        small_pic = pil.open(picio)
        y, x = i // lenx, i % lenx
        outpic.paste(small_pic, (x * 256, y * 256))
    print('Tiles merge completed')
    return outpic


def download_tiles(urls, multi=10):
    url_len = len(urls)
    datas = [None] * url_len
    if multi < 1 or multi > 20 or not isinstance(multi, int):
        raise Exception("multi of Downloader shuold be int and between 1 to 20.")
    tasks = [Downloader(i, multi, urls, datas) for i in range(multi)]
    for i in tasks:
        i.start()
    for i in tasks:
        i.join()
    return datas


# ---------------------------------------------------------

# ---------------------------------------------------------
def main(west, north, east, south, zoom, filePath, server="Google-China"):
    """
    Download images based on spatial extent.

    East longitude is positive and west longitude is negative.
    North latitude is positive, south latitude is negative.

    Parameters
    ----------
    west, north : west-north coordinate, for example (100.361,38.866)
        
    east, south : east-south coordinate
        
    z : zoom

    filePath : File path for storing results, TIFF format
        
    style : 
        m for map; 
        s for satellite; 
        y for satellite with label; 
        t for terrain; 
        p for terrain with label; 
        h for label;
    
    source : Google-China (default) or Google
    """
    style='s';
       
    if math.fabs(west)>360 or math.fabs(north)>90 or math.fabs(east)>360 or math.fabs(south)>90:
        west,north=Utils.mercator_to_wgs(west,north);
        east,south=Utils.mercator_to_wgs(east,south);
    
    # ---------------------------------------------------------
    # Get the urls of all tiles in the extent
    urls = get_urls(west, north, east, south, zoom, server, style)

    # Group URLs based on the number of CPU cores to achieve roughly equal amounts of tasks
    urls_group = [urls[i:i + math.ceil(len(urls) / multiprocessing.cpu_count())] for i in
                  range(0, len(urls), math.ceil(len(urls) / multiprocessing.cpu_count()))]

    # Each set of URLs corresponds to a process for downloading tile maps
    print('Tiles downloading......')
    pool = multiprocessing.Pool(multiprocessing.cpu_count())
    results = pool.map(download_tiles, urls_group)
    pool.close()
    pool.join()
    result = [x for j in results for x in j]
    print('Tiles download complete')

    # Combine downloaded tile maps into one map
    outpic = merge_tiles(result, west, north, east, south, zoom)
    outpic = outpic.convert('RGB')
    r, g, b = cv2.split(np.array(outpic))

    # Get the spatial information of the four corners of the merged map and use it for outputting
    extent = getExtent(west, north, east, south, zoom, server)
    gt = (extent['LT'][0], (extent['RB'][0] - extent['LT'][0]) / r.shape[1], 0, extent['LT'][1], 0,
          (extent['RB'][1] - extent['LT'][1]) / r.shape[0])
    saveTiff(r, g, b, gt, filePath)

# ------------ args define --------------------------------
