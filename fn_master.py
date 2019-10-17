#!/usr/bin/env python
# coding: utf-8
#This script is the global refrerence for all future python scripts that need to run these types of jobs
#The hope is that this saves time and makes the methodology more consistent

#Functions contained are: gridexpand, latlon, resample, var_dict


import numpy as np
import pandas as pd
import netCDF4 as nc
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import pyproj as Proj
from pyresample import image, geometry, SwathDefinition




#A function that turns data at a 2km resolution to data at a 20km resolution,
#using the 'kind' variable to store the type of transformation that needs to be done

def gridexpand(data,kind):

    #Loading in the 2km lat/lon grids
    lat2 = np.load('/localdata/coordinates/2km_lat.npy')
    lon2 = np.load('/localdata/coordinates/2km_lon.npy')
    
    #Filling the masked array with NaNs
    data = np.ma.filled(data,fill_value=np.nan)
    
    #Getting the grid points that we need
    latlen = len(lat2[::10])
    lonlen = len(lon2[::10])
    latpts = range(0,latlen,1)
    lonpts = range(0,lonlen,1)

    #Initializing the new grid
    new_grid = np.ones((latlen,lonlen))
    
    for lat in latpts:#Outer loop is per point of latitude with the new grid
        for lon in lonpts:#Inner loop is per point of longitude with the new grid
            #Selecting the data that we want to
            section = data[lat*10:(lat+1)*10,lon*10:(lon+1)*10]
            if kind == 'max':
                new_grid[lat,lon] = np.nanmax(section)
            elif kind == 'min':
                new_grid[lat,lon] = np.nanmin(section)
            elif kind == 'avg':
                new_grid[lat,lon] = np.nanmean(section)

    ntruther = np.isnan(new_grid)
    new_grid[ntruther] = np.nan        
            
    return new_grid




#A function that takes in a netCDF Dataset and returns its x/y coordinates as as lon/lat
def latlon(data):
    X = data.variables['x'][:]
    Y = data.variables['y'][:]
    
    sat_h = data.variables['goes_imager_projection'].perspective_point_height
    sat_lon = data.variables['goes_imager_projection'].longitude_of_projection_origin
    sat_sweep = data.variables['goes_imager_projection'].sweep_angle_axis
    
    p = Proj(proj='geos', h=sat_h, lon_0=sat_lon, sweep=sat_sweep)
    YY, XX = np.meshgrid(Y, X)
    lons, lats = p(XX, YY, inverse=True)
    
    return lons.T, lats.T




#A funtion that takes in a variable at a 10km resolution and resamples it to 2km
def resample(var):
    #Loading in the 2km and 10km grids
    lat2 = np.load('/localdata/coordinates/2km_lat_grid.npy')
    lon2 = np.load('/localdata/coordinates/2km_lon_grid.npy')
    lat10= np.load('/localdata/coordinates/10km_lat.npy')
    lon10= np.load('/localdata/coordinates/10km_lon.npy')

    #Creating the swath definitions
    swath_def_2km = SwathDefinition(lons=lon2,lats=lat2)
    swath_def_10km = SwathDefinition(lons=lon10,lats=lat10)
    
    #Attaching the variable to the 10km swath with an image container
    swath_con = image.ImageContainerNearest(np.ma.filled(var,fill_value=np.nan),swath_def_10km,radius_of_influence=100000)
    
    #Resampling the data from swath_con at 2km
    swath_resampled = swath_con.resample(swath_def_2km)
    
    #Extracting the data
    result = swath_resampled.image_data
    return result




#A funciton that returns a dictionary to reference for the variable:
#(0) acronyms 
#(1) netCDF variable name 
#(2) title name 
#(3) units
#(4) varexpand type
def var_dict():
    v = {
    'CMIP13':['CMIPC13','CMI'                  ,'ABI Cloud Top Brightness Temperature - Band 13','Brightness Temperature (K)'                  ,'min'],
    'ACHA':  ['ACHAC'  ,'HT'                   ,'ABI Cloud Top Height'                          ,'Cloud Top Height (m)'                        ,'max'],
    'CTP':   ['CTPC'   ,'PRES'                 ,'ABI Cloud Top Pressure'                        ,'Cloud Top Pressure (hPa)'                    ,'min'],
    'ACTP':  ['ACTPC'  ,'Phase'                ,'ABI Cloud Top Phase'                           ,'Phase'                                       ,'max'],
    'FED':   ['FED'    ,'flash_extent_density' ,'GLM Flash Extent Density'                      ,'Flash Extent Density (Flashes per 5 min.)'   ,'max'],
    'FE':    ['FE'     ,'total_energy'         ,'GLM Total Flash Energy'                        ,'Energy (fJ)'                                 ,'max'],
    'AFA':   ['AFA'    ,'average_flash_area'   ,'GLM Average Flash Area'                        ,'Flash Area (km 2)'                           ,'avg'],
    'MFA':   ['MFA'    ,'minimum_flash_area'   ,'GLM Minimum Flash Area'                        ,'Flash Area (km 2)'                           ,'min']
    }
    return v




