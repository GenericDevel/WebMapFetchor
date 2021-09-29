import math
import multiprocessing

import os
import queue
import shutil
import threading
from PIL import Image
from datetime import datetime
from urllib import request


class MapDownloader(object):
    def __init__(self, x_start, y_start, x_end, y_end, zoom=20, tile_size=256):
        # self.tile_server = 'https://mts1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}'
        # self.tile_server = 'http://tile.openstreetmap.org/{z}/{x}/{y}.png'
        self.tile_server = 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}'
 
        #判断输入的坐标是否为经纬度，如果为不是经纬度，则需要转WGS84
        if math.fabs(x_start)>360 or math.fabs(x_end)>360 or math.fabs(y_start)>90 or math.fabs(y_end)>90:
            self.lng_start,self.lat_start=self.webMercator2wgs84(x_start,y_start);
            self.lng_end,self.lat_end=self.webMercator2wgs84(x_end,y_end);
        else:
            self.lat_start = y_start
            self.lng_start = x_start
            self.lat_end = y_end
            self.lng_end = x_end
            
        self.zoom = zoom
        self.tile_size = tile_size

        self.q = queue.Queue()
        self.num_worker = multiprocessing.cpu_count() - 1
            
        self._generate_xy_point()
            
    def wgs84toWebMercator(self,lon,lat):
        '''WGS84转Mercator'''
        x =  lon*20037508.342789/180
        y =math.log(math.tan((90+lat)*math.pi/360))/(math.pi/180)
        y = y *20037508.34789/180
        return x,y
    
    #WebMercator-wgs84
    def webMercator2wgs84(self,x,y):
        '''Mercator转WGS84'''
        lon = x/20037508.34*180
        lat = y/20037508.34*180
        lat= 180/math.pi*(2*math.atan(math.exp(lat*math.pi/180))-math.pi/2)
        return lon,lat
    
    def _generate_xy_point(self):
        '''计算起始瓦片编号'''
        self._x_start, self._y_start = self._convert_latlon_to_xy(self.lat_start, self.lng_start)
        self._x_end, self._y_end = self._convert_latlon_to_xy(self.lat_end, self.lng_end)

    def _convert_latlon_to_xy(self, lat, lng):
        '''根据经纬度计算瓦片编号'''
        tiles_count = 1 << self.zoom

        point_x = (self.tile_size / 2 + lng * self.tile_size / 360.0) * tiles_count // self.tile_size
        sin_y = math.sin(lat * (math.pi / 180.0))
        point_y = ((self.tile_size / 2) + 0.5 * math.log((1 + sin_y) / (1 - sin_y)) *
                   -(self.tile_size / (2 * math.pi))) * tiles_count // self.tile_size

        return int(point_x), int(point_y)

    def _fetch_worker(self):
        '''获取瓦片操作'''
        while True:
            item = self.q.get()
            if item is None:
                break

            idx, url, current_tile = item
            print('Fetching #{} of {}: {}'.format(idx, self.q_size, url))
            request.urlretrieve(url, current_tile)

            self.q.task_done()

    def write_into(self, filename):
        '''保存瓦片'''
        # create temp dir
        directory = os.path.abspath('./{}'.format(datetime.now().strftime("%Y-%m-%d_%H-%M-%S")))
        if not os.path.exists(directory):
            os.makedirs(directory)

        # generate source list
        idx = 1
        for x in range(0, self._x_end + 1 - self._x_start):
            for y in range(0, self._y_end + 1 - self._y_start):
                url = self.tile_server.format(
                    x=str(self._x_start + x), y=str(self._y_start + y), z=str(self.zoom))
                current_tile = os.path.join(directory, 'tile-{}_{}_{}.png'.format(
                    str(self._x_start + x), str(self._y_start + y), str(self.zoom)))
                self.q.put((idx, url, current_tile))
                idx += 1

        # stop workers
        for i in range(self.num_worker):
            self.q.put(None)

        # start fetching tile using multithread to speed up process
        self.q_size = self.q.qsize()

        threads = []
        for i in range(self.num_worker):
            t = threading.Thread(target=self._fetch_worker)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        # combine image into single
        width, height = 256 * (self._x_end + 1 - self._x_start), 256 * (self._y_end + 1 - self._y_start)
        map_img = Image.new('RGB', (width, height))

        for x in range(0, self._x_end + 1 - self._x_start):
            for y in range(0, self._y_end + 1 - self._y_start):
                current_tile = os.path.join(directory, 'tile-{}_{}_{}.png'.format(
                    str(self._x_start + x), str(self._y_start + y), str(self.zoom)))
                im = Image.open(current_tile)
                map_img.paste(im, (x * 256, y * 256))

        map_img.save(filename)

        # remove temp dir
        shutil.rmtree(directory)


def main():
    try:
        #md = MapDownloader(-6.256524, 107.170208, -6.292112, 107.242934, zoom=17)
        #md = MapDownloader(12763042,2706157,12763697,2706831,15);
        md = MapDownloader(114.65,23.61,114.66,23.62,14);
        md.write_into('lemanabang.png')

        print("The map has successfully been created")
    except Exception as e:
        print(
            "Could not generate the image - try adjusting the zoom level and checking your coordinates. Cause: {}".format(
                e))


if __name__ == '__main__':
    main()
