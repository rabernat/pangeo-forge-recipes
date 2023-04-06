import numpy as np
import pandas as pd
import xarray as xr


def make_ds(nt=10, non_dim_coords=False, add_extra_dim_coords=False, use_cftime=False):
    """Return a synthetic random xarray dataset."""
    np.random.seed(2)
    # TODO: change nt to 11 in order to catch the edge case where
    # items_per_input does not evenly divide the length of the sequence dimension
    ny, nx, ne = 18, 36, 2
    if use_cftime:
        time = xr.cftime_range(start="2010-01-01", periods=nt, freq="D")
    else:
        time = pd.date_range(start="2010-01-01", periods=nt, freq="D")
    lon = (np.arange(nx) + 0.5) * 360 / nx
    lon_attrs = {"units": "degrees_east", "long_name": "longitude"}
    lat = (np.arange(ny) + 0.5) * 180 / ny
    lat_attrs = {"units": "degrees_north", "long_name": "latitude"}
    foo = np.random.rand(nt, ny, nx)
    foo_attrs = {"long_name": "Fantastic Foo"}
    # make sure things work with heterogenous data types
    bar = np.random.randint(0, 10, size=(nt, ny, nx))
    bar_attrs = {"long_name": "Beautiful Bar"}
    dims = ("time", "lat", "lon")
    coords = {
        "time": ("time", time),
        "lat": ("lat", lat, lat_attrs),
        "lon": ("lon", lon, lon_attrs),
    }
    if non_dim_coords:
        coords["timestep"] = ("time", np.arange(nt))
        coords["baz"] = (("lat", "lon"), np.random.rand(ny, nx))
    
    if add_extra_dim_coords:
        # introduce a coordinate with a dimension not used in the data variables
        coords["extra_dim_coord"] = (("extra_dim", "time"), np.random.rand(ne, nt))
        coords["extra_dim"] = ("extra_dim", np.arange(ne))

    ds = xr.Dataset(
        {"bar": (dims, bar, bar_attrs), "foo": (dims, foo, foo_attrs)},
        coords=coords,
        attrs={"conventions": "CF 1.6"},
    )

    # Add time coord encoding
    # Remove "%H:%M:%s" as it will be dropped when time is 0:0:0
    # if not use_cftime:
    ds.time.encoding = {
        "units": f"days since {time[0].strftime('%Y-%m-%d')}",
        "calendar": "proleptic_gregorian",
    }

    return ds
