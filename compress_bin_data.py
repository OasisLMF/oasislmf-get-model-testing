import argparse
import logging
import numba as nb
import numpy as np
import os
import sys
from contextlib import ExitStack
from logging import NullHandler
from numba.typed import Dict
import zlib
import json
import subprocess


areaperil_int = np.dtype(os.environ.get('AREAPERIL_TYPE', 'u4'))
oasis_float = np.dtype(os.environ.get('OASIS_FLOAT', 'f4'))
oasis_int_dtype = np.dtype('i4')
oasis_int = np.int32
oasis_int_size = np.int32().itemsize
# buff_int_size = buff_size // oasis_int_size

areaperil_int_relative_size = areaperil_int.itemsize // oasis_int_size
oasis_float_relative_size = oasis_float.itemsize // oasis_int_size
results_relative_size = 2 * oasis_float_relative_size

EventIndexBin = nb.from_dtype(np.dtype([('event_id', np.int32),
                                        ('offset', np.int64),
                                        ('size', np.int64)
                                        ]))

Index_type = nb.from_dtype(np.dtype([('start', np.int64),
                                     ('end', np.int64)
                                     ]))


Event = nb.from_dtype(np.dtype([('areaperil_id', areaperil_int),
                                ('intensity_bin_id', np.int32),
                                ('probability', oasis_float)
                                ]))

event_size = Event.size

footprint_offset = 8

damagebindictionary =  nb.from_dtype(np.dtype([('bin_index', np.int32),
                                               ('bin_from', oasis_float),
                                               ('bin_to', oasis_float),
                                               ('interpolation', oasis_float),
                                               ('interval_type', np.int32),
                                               ]))

damagebindictionaryCsv =  nb.from_dtype(np.dtype([('bin_index', np.int32),
                                                  ('bin_from', oasis_float),
                                                  ('bin_to', oasis_float),
                                                  ('interpolation', oasis_float)]))

EventCSV =  nb.from_dtype(np.dtype([('event_id', np.int32),
                                    ('areaperil_id', areaperil_int),
                                    ('intensity_bin_id', np.int32),
                                    ('probability', oasis_float)
                                    ]))

Item = nb.from_dtype(np.dtype([('id', np.int32),
                               ('coverage_id', np.int32),
                               ('areaperil_id', areaperil_int),
                               ('vulnerability_id', np.int32),
                               ('group_id', np.int32)
                               ]))


Vulnerability = nb.from_dtype(np.dtype([('vulnerability_id', np.int32),
                                        ('intensity_bin_id', np.int32),
                                        ('damage_bin_id', np.int32),
                                        ('probability', oasis_float)
                                        ]))

VulnerabilityIndex = nb.from_dtype(np.dtype([('vulnerability_id', np.int32),
                                             ('offset', np.int64),
                                             ('size', np.int64),
                                             ('original_size', np.int64)
                                             ]))

VulnerabilityRow = nb.from_dtype(np.dtype([('intensity_bin_id', np.int32),
                                           ('damage_bin_id', np.int64),
                                           ('probability', oasis_float)
                                           ]))

vuln_offset = 4


def compress_bin_file(path: str, compression: int) -> None:
    with open(path, "rb") as file:
        data = file.read()

    with open(path.replace(".bin", ".zip"), "wb") as file:
        compressed_data = zlib.compress(data, compression)
        file.write(compressed_data)


def compress_bin_file_command(size: int) -> None:
    with open(f"./configs/{size}_config.json") as file:
        config_data = json.load(file)
    intensity_bins: int = config_data["num_intensity_bins"]

    # compress the footprint data
    subprocess.call(f"footprinttocsv -b ./data/{size}/static/footprint.bin -x ./data/{size}/static/footprint.idx | "
                    f"footprinttobin -b ./data/{size}/static/fooprint.bin.z -x ./data/{size}/static/footprint.idx.z "
                    f"-i {intensity_bins}",
                    shell=True)
    # compress the vulnerability data


def read_in_chunks(file_object, chunk_size=1024):
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data


def stream_decompress(stream):
    dec = zlib.decompressobj(32 + zlib.MAX_WBITS)
    for chunk in stream:
        rv = dec.decompress(chunk)
        if rv:
            yield rv


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", help="the size of the data", type=int)
    args = parser.parse_args()

    compress_bin_file_command(size=args.n)

    # the commands below compress the binary files using zlib in Python, however, the compress_bin_file_command
    # function above uses the builtin compression from the oasislmf command line which has the already defined
    # format. Therefore this is used for now but the others below are commented out at the moment

    # compress_bin_file(f"./data/{args.n}/events.bin", args.c)
    # compress_bin_file(f"./data/{args.n}/input/events.bin", args.c)
    # compress_bin_file(f"./data/{args.n}/static/footprint.bin", args.c)
    # # compress_bin_file("./data/bin700/input/footprint.bin")
    # compress_bin_file(f"./data/{args.n}/static/vulnerability.bin", args.c)
    # compress_bin_file(f"./data/{args.n}/static/damage_bin_dict.bin", args.c)
