# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # METHOD TO ROTATE THE u/v WINDS DATA TO THE EARTH CENTRIC COORDS AND NOT THE GRID CENTRIC
# # #  accessed at: https://www.atmos.washington.edu/~ovens/wrfwinds.html
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

from netCDF4 import Dataset as NetCDFFile
# from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
import numpy as np
import sys

# allow specification of the wrfout file on the command line.
if (len(sys.argv) > 1):
    ncfile = sys.argv[1]
else:
    # ncfile = 'wrfout_d1.uvrewrite.nc'
    template = NetCDFFile('/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_raw_output_example/wrfout_d01_2025-07-10_00:00:00')

    ncfile = '/storage01/pbieniek/ccsm/rcp85/hourly/2006/WRFDS_d01.2006-03-31_08.nc'
print('Using wrfout file: "','\b'+ncfile,'\b"') # use \b to delete space
nc     = NetCDFFile(ncfile, 'r')
#
# CEN_LAT and CEN_LON are specific to the nest and are not
# actually what you want.  If available, you should use
# MOAD_CEN_LAT and STAND_LON
cenlat = template.getncattr('CEN_LAT')
cenlon = template.getncattr('CEN_LON')
cenlon = template.getncattr('STAND_LON')
cenlat = template.getncattr('MOAD_CEN_LAT')
lat1   = template.getncattr('TRUELAT1')
lat2   = template.getncattr('TRUELAT2')
#
# get the actual longitudes, latitudes, and corners
lons = template.variables['XLONG'][0]
lats = template.variables['XLAT'][0]
lllon = lons[0,0]
lllat = lats[0,0]
urlon = lons[-1,-1]
urlat = lats[-1,-1]
#
# get the grid-relative wind components
ur = template.variables['U10'][0]
vr = template.variables['V10'][0]
#
# get the WRF local cosine and sines of the map rotation
cosalpha = template.variables['COSALPHA'][0]
sinalpha = template.variables['SINALPHA'][0]
#
# get the temperature field to plot a background color contour
t2m = nc.variables['T2'][0]
#
# Make our map which is on a completely different projection.
# To illustrate the problems, go to the upper right corner of the domain,
# or lower right corner in the southern hemisphere, where the distortion
# is the greatest
# if urlat >= 0:
#     map = Basemap(projection='cyl',llcrnrlat=urlat-5,urcrnrlat=urlat+3,
#                 llcrnrlon=urlon-10,urcrnrlon=urlon,
#                 resolution='h')
# else:
#     map = Basemap(projection='cyl',llcrnrlat=lllat-5,urcrnrlat=lllat+3,
#                 llcrnrlon=lllon,urcrnrlon=lllon+10,
#                 resolution='h')
# #
# x, y = map(lons[:,:], lats[:,:])
# map.contourf(x, y, t2m, alpha = 0.4) 
# #
# # overlay wind barbs doing nothing to show the problem
# map.barbs(x, y, ur, vr, color='green',label='Unrotated - wrong')
# rotate winds to earth-relative using the correct formulas
ue = ur * cosalpha - vr * sinalpha
ve = vr * cosalpha + ur * sinalpha

# # # ADDED
import wrf
wrf.uvmet(ur, vr, lats, lons, cenlon, cone=1.0, meta=True, units='m s-1')

# # # END ADDED 


# overlay wind barbs using the earth-relative winds showing we're still wrong
map.barbs(x, y, ue, ve, color='red',label='Rotated to latlon - insufficient')
# now rotate our earth-relative winds to the x/y space of this map projection
urot, vrot = map.rotate_vector(ue,ve,lons,lats)
map.barbs(x, y, urot, vrot, color='blue',label='Rotated to latlon and then to map - CORRECT')
# finish up map drawing
parallels = np.arange(-85.,85,1.)
meridians = np.arange(-180.,180.,1.)
map.drawcoastlines(color = '0.15')
map.drawparallels(parallels,labels=[False,True,True,False])
map.drawmeridians(meridians,labels=[True,False,False,True])
# add a legend.
# NOTE: for wind barb/vector plots, each component of the wind got its own 
# legend entry, so this line
#   plt.legend()
# resulted in duplication.  The next 3 lines handle sifting the legend.
ax = plt.gca()
handles, labels = ax.get_legend_handles_labels()
legend = plt.legend([handles[0],handles[2],handles[4]], [labels[0],labels[2],labels[4]])
#
# plot the points of the grid to illustrate where the vectors should be pointing
map.plot(x,y,'bo')
plt.title('How to properly rotate WRF winds to Earth-relative coordinates\nand then reproject them to a new map projection.\n')
plt.show()
  
# For the full U and V fields, they need to be unstaggered first. This can be done easily in python using this:
 U = nc.variables['U'][0]
 V = nc.variables['V'][0]
# destagger ala NCL
 u_unstaggered = 0.5 * (U[:,:,:-1] + U[:,:,1:])
 v_unstaggered = 0.5 * (V[:,:-1,:] + V[:,1:,:])
  