"""Lookup tables for stacking the WRF data.

"""

import numpy as np


# a lookup for mapping the WRF groups (model/scenario)
# to other useful info.
groups = {
    "erain_hist": {
        "years": np.arange(1979, 2018)
    },
    "gfdl_hist": {
        "years": np.arange(1970, 2006)
    },
    "ccsm_hist": {
        "years": np.arange(1970, 2005)
    },
    "gfdl_rcp85": {
        "years": np.arange(2006, 2100)
    },
    "ccsm_rcp85": {
        "years": np.arange(2005, 2100)
    },
}
