import warnings

import numpy as np
import xarray as xr
import pandas as pd
import dask.bag as db


# -------- COMPUTE MAX DIMS --------


def identify_non_gen_vars(lsls):
    """ returns the list of elements
    which are not shared by all the lists in a list """
    # flatten list
    flat_list = [
        x
        for xs in lsls
        for x in xs
    ]

    dict_count = dict.fromkeys(np.unique(flat_list))
    for dim in list(dict_count.keys()):
        dict_count[dim] = flat_list.count(dim)

    iter_count = dict.fromkeys(np.unique(list(dict_count.values())))
    for v in list(iter_count.keys()):
        iter_count[v] = (list(dict_count.values()).count(v))

    # get the max values
    max_val = max(iter_count, key=iter_count.get)

    # get the differnt keys
    different_keys = [key for key, value in dict_count.items() if value != max_val]

    # unique unshared dims values
    uniq_values = list(dict.fromkeys(flat_list))

    # keys shared by
    shared_keys = [item for item in uniq_values if item not in different_keys]

    return shared_keys, different_keys


def get_dims_info(args):
    """ Get Argo profile Vertical dimension name and size """
    ds_name, dim_name = args
    ds = xr.open_dataset(ds_name)
    return dim_name, ds.sizes[dim_name]


def dims_info(ds_name):
    dims = list(dict(xr.open_dataset(ds_name).sizes).keys())
    return dims


def get_dims_max(dss):
    """ return th max of each dimension size on an argo floats list"""
    # list of dimensions availables
    # TODO : à sortir probablement
    vertical_dim_names = ["N_PROF"]

    # list of each file dimensions
    bag = db.from_sequence(dss)
    res = bag.map(dims_info)
    lsls = res.compute()

    dims_names, undesirable_dimensions = identify_non_gen_vars(lsls)

    # on connait maintenant les dimensions partagées par tous les fichiers :
    # TODO : [IMPROVEMENT] - this loop can be mapped and //
    cnt = 0
    max_sizes = dict.fromkeys(dims_names)
    for dim_name in dims_names :
        args = list(zip(dss, [dim_name]*len(dss)))
        bag = db.from_sequence(args)
        res = bag.map(get_dims_info)
        names_and_sizes = res.compute()
        sizes = list(zip(*names_and_sizes))[1]

        max_sizes[dim_name] = max(sizes)

        # find vertical dimension :
        if dim_name in vertical_dim_names:
            z_axis = dim_name
            cnt += 1

        if cnt > 1:
            raise ValueError(
                " More than one particiant to the Z axis position .. AÏE! :O"
                )

    if cnt == 0:
            raise ValueError(
                "No job application for the Z-axis position... please consider edit the reader ;) "
                )

    return max_sizes, z_axis, undesirable_dimensions


# -------- EXPAND AND CAT DIMS --------


def compute_n_missing_lines(ds, max_n_levels):
    keys = list(dict(ds.sizes).keys())
    max_n_levels = np.array(list(max_n_levels.values()))
    ds_n_levels = np.array(list(dict(ds.sizes).values()))

    diff = max_n_levels-ds_n_levels

    return dict(zip(keys, diff))


def find_variables_with_dimension(ds, dim_name):
    """
    ~~~ Chat GPT function ~~~
    Find variables in an xarray dataset that are associated with a specific dimension.

    Parameters:
    ds (xarray.Dataset): The xarray dataset.
    dim_name (str): The dimension name to check.

    Returns:
    list: A list of variable names that have the specified dimension.
    """
    variables_with_dim = [var for var in ds.data_vars if dim_name in ds[var].dims]
    return variables_with_dim


def concat_2nd(args):
    """ on cree un patch de nan qu'on concatene dans toutes le dimensions
    les unes apres les autreqs pour eviter les erreurs de merge """
    ds_name, max_n_levels, z_axis, undesirable_dimensions = args
    # initiate ds and patch ds
    ds = xr.open_dataset(ds_name)

    # remove vars concerned by the undesirable dimensions
    if undesirable_dimensions != None :
        for dim in undesirable_dimensions :
            ds = ds.drop_vars(find_variables_with_dimension(ds, dim), errors='raise')

    # compute number of missing lines
    nb_missing_lines = compute_n_missing_lines(ds, max_n_levels)
    # on enleve la dimension sur laquell on va concatener
    nb_missing_lines[z_axis]=0

    # instanciate a new empty dataset
    new_ds = xr.Dataset()
    for dim, dim_size in dict(ds.sizes).items():
        if nb_missing_lines[dim] == 0 :
            new_ds[dim] = xr.DataArray(data=np.arange(dim_size), dims=dim)
        else :
            new_ds[dim] = xr.DataArray(data=np.arange(max_n_levels[dim]), dims=dim)

    # --- extend dimensions into new_dict ---
    # TODO : [IMPROVEMENT] - this loop can be mapped and //
    for var in ds.data_vars:
        # creating dict for padding
        if not ds[var].sizes:
            padding_dict = {z_axis: (0, 1)}
        else :
            padding_dict = {}
            for vardim in ds[var].sizes:
                padding_dict[vardim] = (0, nb_missing_lines[vardim])

        # pad each variable to match new dataset dimensions
        new_ds[var] = ds[var].pad(pad_width=padding_dict, constant_values=None)

    # drop initial variables
    new_ds = new_ds.drop_vars(list(ds.sizes.keys()))

    # copy attributes from old dataset
    new_ds.attrs = ds.attrs

    return new_ds


def combine_ds(dss):
    """ dss = list f files """
    max_n_levels, z_axis, undesirable_dimensions = get_dims_max(dss)

    args = list(zip(dss, [max_n_levels]*len(dss), [z_axis]*len(dss), [undesirable_dimensions]*len(dss)))

    bag = db.from_sequence(args)
    res = bag.map(concat_2nd)
    res_computed = res.compute()

    aggregated_dataset = xr.concat(res_computed, dim=z_axis)

    return aggregated_dataset


def extract_meta(dss):
    """ dss = list f files """
    if any("meta" in s for s in dss):
        meta_file = [s for s in dss if "meta" in s]
        dss.remove(meta_file[0])
        return meta_file[0], dss
    else:
        return None, dss

def include_meta(meta_file, aggregated_dataset):
    """ adding additional information only present in the meta file """
    if meta_file:
        ds_meta = xr.open_dataset(meta_file)
        try:
            launch_date = np.array([ds_meta["LAUNCH_DATE"].astype(str).data])[0].strip()
            aggregated_dataset.attrs["launch_date"] = str(pd.to_datetime(launch_date, format='%Y%m%d%H%M%S'))
        except:
            raise warnings.warn('no LAUNCH_DATE found in meta file')

        try:
            aggregated_dataset.attrs["platform_type"] = np.array([ds_meta["PLATFORM_TYPE"].astype(str).data])[0].strip()
        except:
            raise warnings.warn('no PLATFORM_TYPE found in meta file')

        return aggregated_dataset

    else:
        return aggregated_dataset


def cat_datasets(args):
    # starting by extracting meta files and list of files
    meta_file, dss = extract_meta(*args)

    # si le fichier meta existe on l'inclus
    # if meta_file:
    # CASE : ARGO FILES TYPE
    aggregated_dataset = combine_ds(dss)
    aggregated_dataset = include_meta(meta_file, aggregated_dataset)

    return aggregated_dataset
