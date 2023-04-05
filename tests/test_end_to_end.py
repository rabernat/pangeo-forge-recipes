import os

import apache_beam as beam
import pytest
import xarray as xr
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline

from pangeo_forge_recipes.transforms import OpenWithXarray, StoreToZarr

# from apache_beam.testing.util import assert_that, equal_to
# from apache_beam.testing.util import BeamAssertException, assert_that, is_not_empty


@pytest.fixture
def pipeline():
    options = PipelineOptions(runtime_type_check=False)
    with TestPipeline(options=options) as p:
        yield p


@pytest.mark.parametrize("target_chunks", [{"time": 1}, {"time": 2}, {"time": 3}])
def test_xarray_zarr(
    daily_xarray_dataset,
    netcdf_local_file_pattern_sequential,
    pipeline,
    tmp_target_url,
    target_chunks,
):
    pattern = netcdf_local_file_pattern_sequential
    with pipeline as p:
        (
            p
            | beam.Create(pattern.items())
            | OpenWithXarray(file_type=pattern.file_type)
            | StoreToZarr(
                target_root=tmp_target_url,
                store_name="store",
                target_chunks=target_chunks,
                combine_dims=pattern.combine_dim_keys,
            )
        )

    ds = xr.open_dataset(os.path.join(tmp_target_url, "store"), engine="zarr")
    assert ds.time.encoding["chunks"] == (target_chunks["time"],)
    xr.testing.assert_equal(ds.load(), daily_xarray_dataset)


def test_xarray_zarr_subpath(
    daily_xarray_dataset,
    netcdf_local_file_pattern_sequential,
    pipeline,
    tmp_target_url,
):
    pattern = netcdf_local_file_pattern_sequential
    with pipeline as p:
        (
            p
            | beam.Create(pattern.items())
            | OpenWithXarray(file_type=pattern.file_type)
            | StoreToZarr(
                target_root=tmp_target_url,
                store_name="subpath",
                combine_dims=pattern.combine_dim_keys,
            )
        )

    ds = xr.open_dataset(os.path.join(tmp_target_url, "subpath"), engine="zarr")
    xr.testing.assert_equal(ds.load(), daily_xarray_dataset)

# from .data_generation import make_ds
# def test_failure_chunk_regions():
#     ds = make_ds(non_dim_coords=True, add_extra_dim_coords=True)
#     print(ds)
#     assert False

    #     # create a dummy dataset similar to https://github.com/pangeo-forge/pangeo-forge-recipes/issues/504
    # nx, ny, nt, nb = 3, 5, 10, 2
    # data = xr.DataArray(np.random.rand(nx, ny, nt), dims=["x", "y", "time"])
    # true_coord = xr.DataArray(np.random.rand(nx, ny), dims=["x", "y"])
    # issue_coord = xr.DataArray(np.random.rand(nt, nb), dims=["time", 'bnds'])
    # # ds = xr.Dataset(
    # #     {'data': data, 'issue_coord': issue_coord}, coords={'true_coord': true_coord}
    # #     )
    # ds = xr.Dataset({'data': data}, coords={'true_coord': true_coord, 'issue_coord': issue_coord})
    # schema = dataset_to_schema(ds)
    # print(determine_target_chunks(schema, specified_chunks={'time': 1, 'x': nx, 'y': ny, 'bnds': nb}))
    # print(ds)