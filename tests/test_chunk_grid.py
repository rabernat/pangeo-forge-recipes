import pytest

from pangeo_forge_recipes.chunk_grid import ChunkAxis, ChunkGrid


def test_chunk_axis():
    ca = ChunkAxis(chunks=(2, 4, 3))
    assert len(ca) == 9
    assert ca.nchunks == 3

    # yes we could parameterize this but writing it out helps understanding
    with pytest.raises(IndexError):
        _ = ca.array_index_to_chunk_index(-1)
    assert ca.array_index_to_chunk_index(0) == 0
    assert ca.array_index_to_chunk_index(1) == 0
    assert ca.array_index_to_chunk_index(2) == 1
    assert ca.array_index_to_chunk_index(3) == 1
    assert ca.array_index_to_chunk_index(4) == 1
    assert ca.array_index_to_chunk_index(5) == 1
    assert ca.array_index_to_chunk_index(6) == 2
    assert ca.array_index_to_chunk_index(7) == 2
    assert ca.array_index_to_chunk_index(8) == 2
    with pytest.raises(IndexError):
        _ = ca.array_index_to_chunk_index(9)

    bad_array_slices = slice(0, 5, 2), slice(-1, 5), slice(5, 4), slice(5, 10)
    for sl in bad_array_slices:
        with pytest.raises(IndexError):
            _ = ca.array_slice_to_chunk_slice(sl)

    assert ca.array_slice_to_chunk_slice(slice(0, 9)) == slice(0, 3)
    assert ca.array_slice_to_chunk_slice(slice(1, 9)) == slice(0, 3)
    assert ca.array_slice_to_chunk_slice(slice(2, 9)) == slice(1, 3)
    assert ca.array_slice_to_chunk_slice(slice(2, 8)) == slice(1, 3)
    assert ca.array_slice_to_chunk_slice(slice(2, 6)) == slice(1, 2)
    assert ca.array_slice_to_chunk_slice(slice(2, 5)) == slice(1, 2)
    assert ca.array_slice_to_chunk_slice(slice(6, 7)) == slice(2, 3)

    with pytest.raises(IndexError):
        _ = ca.chunk_index_to_array_slice(-1)
    assert ca.chunk_index_to_array_slice(0) == slice(0, 2)
    assert ca.chunk_index_to_array_slice(1) == slice(2, 6)
    assert ca.chunk_index_to_array_slice(2) == slice(6, 9)
    with pytest.raises(IndexError):
        _ = ca.chunk_index_to_array_slice(3)


def test_chunk_axis_subsets():
    ca = ChunkAxis(chunks=(2, 4, 3))
    cas = ca.subset(2)
    assert cas.chunks == (1, 1, 2, 2, 1, 2)


def test_chunk_axis_consolidate():
    ca = ChunkAxis(chunks=(2, 4, 3, 4, 2))
    cac = ca.consolidate(2)
    assert cac.chunks == (6, 7, 2)
    cad = ca.consolidate(3)
    assert cad.chunks == (9, 6)


def test_chunk_grid():
    cg = ChunkGrid({"x": (2, 4, 3), "time": (7, 8)})
    assert cg.dims == {"x", "time"}
    assert cg.shape == {"x": 9, "time": 15}
    assert cg.nchunks == {"x": 3, "time": 2}
    assert cg.ndim == 2

    assert cg.array_index_to_chunk_index({"x": 2}) == {"x": 1}
    assert cg.array_index_to_chunk_index({"time": 10}) == {"time": 1}
    assert cg.array_index_to_chunk_index({"x": 7, "time": 10}) == {"x": 2, "time": 1}

    assert cg.array_slice_to_chunk_slice({"x": slice(0, 9)}) == {"x": slice(0, 3)}
    assert cg.array_slice_to_chunk_slice({"time": slice(0, 15)}) == {"time": slice(0, 2)}
    assert cg.array_slice_to_chunk_slice({"x": slice(0, 9), "time": slice(0, 15)}) == {
        "x": slice(0, 3),
        "time": slice(0, 2),
    }

    assert cg.chunk_index_to_array_slice({"x": 1}) == {"x": slice(2, 6)}
    assert cg.chunk_index_to_array_slice({"time": 1}) == {"time": slice(7, 15)}
    assert cg.chunk_index_to_array_slice({"x": 1, "time": 1}) == {
        "x": slice(2, 6),
        "time": slice(7, 15),
    }


def test_chunk_grid_from_uniform_grid():
    cg1 = ChunkGrid({"x": (2, 2), "y": (3, 3, 3, 1)})
    cg2 = ChunkGrid.from_uniform_grid({"x": (2, 4), "y": (3, 10)})
    assert cg1 == cg2
