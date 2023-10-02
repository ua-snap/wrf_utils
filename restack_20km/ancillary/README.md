# Ancillary code / notebooks for the re-stack 20 km WRF pipeline

This folder contains Jupyter notebooks that serve an exploratory purpose and serve as records to inform decisions about the code in the pipeline.

### Folders

- `eval_prod_comparison`: This folder contains notebooks that explore the results from running the `../../prod_comparison.ipynb` notebook on each WRF group after the restacking was done. These files will serve as references for logging the differences / discontinuities between the existing production (AWS) data and the newly restacked data. 

### Files

- `explore_wind_data_corruption_issue.ipynb`: explores the possibly corrupted wind data revealed by P. Bieniek in Dec, 2021.  
- `include_latlon.ipynb`: explores what it will take to create output files with the lat / lon grids and proper spatial info for opening in various GIS software packages  
- `wrf_cleanup_20211008.ipynb`: explores the data present on `/import/SNAP/wrf_data/` (Chinook) / `/rcs/` (Atlas), motivated by dynamical downscaling  group's need to free up space.   
- `repair_winds_rotation_issue.ipynb`: explores the cause of the issue of incorrectly rotated winds discovered by P. Bieniek in Dec 2020 and demonstrates the repair.  
