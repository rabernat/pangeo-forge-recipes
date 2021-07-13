"""
Filename / URL patterns.
"""

import warnings
from dataclasses import dataclass, field, replace
from itertools import product
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple, Union


@dataclass
class SubsetSpec:
    """A data structure that explains how to subset a file in one dimension.
    """

    dim_name: str
    this_segment: int
    total_segments: int

    def __post_init__(self):
        assert self.total_segments > 0
        assert self.this_segment >= 0
        assert self.this_segment < self.total_segments


@dataclass
class OpenSpec:
    """A data structure that explains how to open a file.
    """

    fname: str
    subsets: List[SubsetSpec] = field(default_factory=list)


@dataclass
class ConcatDim:
    """Represents a concatenation operation across a dimension of a FilePattern.

    :param name: The name of the dimension we are concatenating over. For
      files with labeled dimensions, this should match the dimension name
      within the file. The most common value is ``"time"``.
    :param keys: The keys used to represent each individual item along this
      dimension. This will be used by a ``FilePattern`` object to evaluate
      the file name.
    :param nitems_per_file: If each file contains the exact same known number of
      items in each file along the concat dimension, this can be set to
      provide a fast path for recipes.
    """

    name: str  # should match the actual dimension name
    keys: Sequence[Any] = field(repr=False)
    nitems_per_file: Optional[int] = None


@dataclass
class MergeDim:
    """Represents a merge operation across a dimension of a FilePattern.

    :param name: The name of the dimension we are are merging over. The actual
       value is not used by most recipes. The most common value is
       ``"variable"``.
    :param keys: The keys used to represent each individual item along this
      dimension. This will be used by a ``FilePattern`` object to evaluate
      the file name.
    """

    name: str
    keys: Sequence[Any] = field(repr=False)


Index = Tuple[int, ...]
CombineDim = Union[MergeDim, ConcatDim]


@dataclass
class SubsetDim:
    """Adds an layer of iteration to represent subsetting within each file.

    :param dim: The name of the dimension we are subsetting over.
    :param subset_factor: How many pieces to divide each segment into.
    """

    dim: str  # should match the actual dimension name we are subsetting over
    subset_factor: int

    @property
    def name(self):
        return f"{self.dim}_subset"

    @property
    def keys(self):
        return list(range(self.subset_factor))


class FilePattern:
    """Represents an n-dimensional matrix of individual files to be combined
    through a combination of merge and concat operations. Each operation generates
    a new dimension to the matrix.

    :param format_function: A function that takes one argument for each
      non-subset combine_op and returns a filename.
      Each argument name should correspond to a ``name`` in the ``combine_dims``
      list.
    :param combine_dims: A sequence of concat, merge, or subset dimensions.
    """

    def __init__(self, format_function: Callable, *combine_dims: CombineDim):
        self.format_function = format_function
        self.combine_dims = combine_dims

    def __repr__(self):
        return f"<FilePattern {self.dims}>"

    @property
    def dims(self) -> Dict[str, int]:
        """Dictionary representing the dimensions of the FilePattern. Keys are
        dimension names, values are the number of items along each dimension."""
        return {op.name: len(op.keys) for op in self.combine_dims}

    @property
    def shape(self) -> Tuple[int, ...]:
        """Shape of the filename matrix."""
        return tuple([len(op.keys) for op in self.combine_dims])

    @property
    def merge_dims(self) -> List[str]:
        """List of dims that are merge operations"""
        return [op.name for op in self.combine_dims if isinstance(op, MergeDim)]

    @property
    def concat_dims(self) -> List[str]:
        """List of dims that are concat operations"""
        return [op.name for op in self.combine_dims if isinstance(op, ConcatDim)]

    @property
    def subset_dims(self) -> List[str]:
        return [op.name for op in self.combine_dims if isinstance(op, SubsetDim)]

    @property
    def nitems_per_input(self) -> Dict[str, Union[int, None]]:
        """Dictionary mapping concat dims to number of items per file."""
        nitems = {}  # type: Dict[str, Union[int, None]]
        for op in self.combine_dims:
            if isinstance(op, ConcatDim):
                if op.nitems_per_file:
                    nitems[op.name] = op.nitems_per_file
                else:
                    nitems[op.name] = None
        return nitems

    @property
    def concat_sequence_lens(self) -> Dict[str, Optional[int]]:
        """Dictionary mapping concat dims to sequence lengths.
        Only available if ``nitems_per_input`` is set on the dimension."""
        return {
            dim_name: (nitems * self.dims[dim_name] if nitems is not None else None)
            for dim_name, nitems in self.nitems_per_input.items()
        }

    def __getitem__(self, indexer) -> OpenSpec:
        """Get a filename path for a particular key. """
        assert len(indexer) == len(self.combine_dims)
        format_function_kwargs = {
            cdim.name: cdim.keys[i]
            for cdim, i in zip(self.combine_dims, indexer)
            if not isinstance(cdim, SubsetDim)
        }
        fname = self.format_function(**format_function_kwargs)
        subset_specs = [
            SubsetSpec(dim_name=cdim.dim, this_segment=i, total_segments=cdim.subset_factor)
            for cdim, i in zip(self.combine_dims, indexer)
            if isinstance(cdim, SubsetDim)
        ]
        return OpenSpec(fname, subset_specs)

    def __iter__(self) -> Iterator[Index]:
        """Iterate over all keys in the pattern. """
        for val in product(*[range(n) for n in self.shape]):
            yield val

    def items(self):
        """Iterate over key, filename pairs."""
        for key in self:
            yield key, self[key]


def pattern_from_file_sequence(file_list, concat_dim, nitems_per_file=None):
    """Convenience function for creating a FilePattern from a list of files."""
    warnings.warn(
        "This function will be removed in a future version. "
        "Please define a FilePattern directly instead.",
        DeprecationWarning,
    )

    keys = list(range(len(file_list)))
    concat = ConcatDim(name=concat_dim, keys=keys, nitems_per_file=nitems_per_file)

    def format_function(**kwargs):
        return file_list[kwargs[concat_dim]]

    return FilePattern(format_function, concat)


def prune_pattern(fp: FilePattern, nkeep: int = 2) -> FilePattern:
    """
    Create a smaller pattern from a full pattern.
    Keeps all MergeDims but only the first `nkeep` items from each ConcatDim

    :param fp: The original pattern.
    :param nkeep: The number of items to keep from each ConcatDim sequence.
    """

    new_combine_dims = []  # type: List[CombineDim]
    for cdim in fp.combine_dims:
        if isinstance(cdim, MergeDim):
            new_combine_dims.append(cdim)
        elif isinstance(cdim, ConcatDim):
            new_keys = cdim.keys[:nkeep]
            new_cdim = replace(cdim, keys=new_keys)
            new_combine_dims.append(new_cdim)
        else:  # pragma: no cover
            assert "Should never happen"

    return FilePattern(fp.format_function, *new_combine_dims)
