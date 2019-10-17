#!/usr/bin/env python
# coding: utf-8

# ## Code designed to compile GLM data over 5-minute intervals




import netCDF4 as nc
import matplotlib.pyplot as plt
import numpy as np
import os
from datetime import datetime as DT
import subprocess as sp
import sys


#===========================================
#Function Land
#===========================================



def magic(position,variable,command):
    #Opening up each of the files
    file5 = nc.Dataset(file_loc+file_list[position],'r')
    file4 = nc.Dataset(file_loc+file_list[position-1],'r')
    file3 = nc.Dataset(file_loc+file_list[position-2],'r')
    file2 = nc.Dataset(file_loc+file_list[position-3],'r')
    file1 = nc.Dataset(file_loc+file_list[position-4],'r')
    
    #Extracting the variable and filling the masked arrays with Nan
    var5 = file5.variables[variable][:,:].astype(np.float32)
    var4 = file4.variables[variable][:,:].astype(np.float32)
    var3 = file3.variables[variable][:,:].astype(np.float32)
    var2 = file2.variables[variable][:,:].astype(np.float32)
    var1 = file1.variables[variable][:,:].astype(np.float32)
    
    file5.close()
    file4.close()
    file3.close()
    file2.close()
    file1.close()
    #Filling the masked arrays with zeros to do the calculations
    var5 = np.ma.filled(var5,fill_value=np.nan)
    var4 = np.ma.filled(var4,fill_value=np.nan)
    var3 = np.ma.filled(var3,fill_value=np.nan)
    var2 = np.ma.filled(var2,fill_value=np.nan)
    var1 = np.ma.filled(var1,fill_value=np.nan)
    
    
    #Stacking all the variables
    stack_var = np.stack((var5,var4,var3,var2,var1),axis=2)
    
    #Making the composite or sums
    if command == 'max':
        final_var = np.nanmax(stack_var,axis=2)
    elif command == 'sum':
        final_var = np.nansum(stack_var,axis=2)
    elif command == 'min':
        final_var = np.nanmin(stack_var,axis=2)
    elif command == 'avg':
        final_var = np.nanmean(stack_var,axis=2)
    
    masker1 = np.isnan(final_var)
    final_var[masker1] = 0
    
    masker2 = final_var == 0
    final_var = np.ma.masked_array(final_var,mask=masker2,fill_value=-999)
    
    return final_var





def time_name(i):
    file = nc.Dataset(file_loc+file_list[i],'r')
    time = file.time_coverage_end
    file.close()
    
    datetime = DT.strptime(time, '%Y-%m-%dT%H:%M:%SZ')
    print (datetime)
    save_name = DT.strftime(datetime,'GLM5-%Y%m%d-%H%M')
    
    return save_name





def ncsave(FED,FCD,AFA,TE,GED,GCD,AGA,MFA,save_name):
    #Setting up the file with the save location
    rootgrp = nc.Dataset(save_loc+save_name+".nc", "w", format="NETCDF4")
    #Creating x and y dimensions
    x = rootgrp.createDimension('x',2500)
    y = rootgrp.createDimension('y',1500)
    #Setting up the variables
    var1 = rootgrp.createVariable(variables[0],'f4',('y','x'))
    #var2 = rootgrp.createVariable(variables[1],'f4',('y','x'))
    var3 = rootgrp.createVariable(variables[2],'f4',('y','x'))
    var4 = rootgrp.createVariable(variables[3],'f4',('y','x'))
    #var5 = rootgrp.createVariable(variables[4],'f4',('y','x'))
    #var6 = rootgrp.createVariable(variables[5],'f4',('y','x'))
    #var7 = rootgrp.createVariable(variables[6],'f4',('y','x'))
    var8 = rootgrp.createVariable(variables[7],'f4',('y','x'))
    #Writing data to the variables
    var1[:,:] = FED
    #var2[:,:] = FCD
    var3[:,:] = AFA
    var4[:,:] = TE
    #var5[:,:] = GED
    #var6[:,:] = GCD
    #var7[:,:] = AGA
    var8[:,:] = MFA
    
    rootgrp.close()
    return 0




#===========================================
#Use like this: python GLM5_min.py 20190414
#===========================================


args = sys.argv

case = args[1]



file_loc = '/localdata/cases/'+case+'/GLM_grids/'
file_list = sorted(os.listdir(file_loc))
save_loc = '/localdata/cases/'+case+'/GLM5/'

if not os.path.exists(save_loc):
            os.makedirs(save_loc) 

counter = np.arange(4,len(file_list))
amax = 'max'
asum = 'sum'
amin = 'min'
aavg = 'avg'
variables = ['flash_extent_density','flash_centroid_density','average_flash_area','total_energy','group_extent_density','group_centroid_density','average_group_area','minimum_flash_area']



for i in counter[::5]:

    save_name = time_name(i)
    
    FED = magic(i,variables[0],asum)
    FCD = magic(i,variables[1],asum)
    AFA = magic(i,variables[2],aavg)
    TE  = magic(i,variables[3],asum)
    GED = magic(i,variables[4],asum)
    GCD = magic(i,variables[5],asum)
    AGA = magic(i,variables[6],aavg)
    MFA = magic(i,variables[7],amin)
    
    ncsave(FED,FCD,AFA,TE,GED,GCD,AGA,MFA,save_name)
    













