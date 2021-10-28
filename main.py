import argparse
import json
import matplotlib.pyplot as plt
import os
import subprocess
from pathlib import Path
from tqdm import tqdm
from typing import List, Tuple
from compress_bin_data import compress_bin_file_command


config_template = {
    "num_vulnerabilities": 50,
    "num_intensity_bins": 50,
    "num_damage_bins": 50,
    "vulnerability_sparseness": 0.5,
    "num_events": 100000,
    "num_areaperils": 100,
    "areaperils_per_event": 100,
    "intensity_sparseness": 0.5,
    "num_periods": 1000,
    "num_locations": 1000,
    "coverages_per_location": 3,
    "num_layers": 1
}


def get_data_path(size: int) -> str:
    return str(os.getcwd()) + "/data/" + str(size)


def get_config_path(size: int) -> str:
    return str(os.getcwd()) + "/configs/" + str(size) + "_config.json"


def write_configs(sizes: List[int]) -> None:
    for size in sizes:
        path = Path(get_data_path(size=size))
        if not path.is_file():
            placeholder = config_template
            placeholder["num_events"] = size
            with open(get_config_path(size=size), "w") as outfile:
                json.dump(placeholder, outfile)


def generate_data(size: int) -> None:
    path = Path(get_data_path(size=size))
    if not path.is_dir():
        subprocess.call(f"sh ./generate_data.sh {size}", shell=True)
        compress_bin_files(size=size)


def compress_bin_files(size: int) -> None:
    subprocess.call(f"python ./compress_bin_data.py -n {size}", shell=True)


def run_the_processes_for_c(size: int) -> float:
    result = subprocess.Popen(f"sh ./run_c_model.sh {size}", shell=True,
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout, stderr = result.communicate()
    time = float(stdout.decode('utf-8'))
    return time


def run_the_processes_for_python(size: int) -> float:
    result = subprocess.Popen(f"sh ./run_python_model.sh {size}", shell=True,
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout, stderr = result.communicate()
    time = float(stdout.decode('utf-8'))
    return time


def plot_outcomes():
    pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", help="run models on the data", type=bool, default=False)
    args = parser.parse_args()

    subprocess.call("sh ./setup.sh", shell=True)
    config_sizes = [100, 500, 600, 700, 800, 900, 1000]

    write_configs(sizes=config_sizes)

    size_data = []
    c_time_data = []
    python_time_data = []

    for i in tqdm(range(len(config_sizes))):
        input_size = config_sizes[i]
        print(f"preparing data for size {input_size}")
        generate_data(size=input_size)
        compress_bin_file_command(size=input_size)

    if args.r is True:
        for i in tqdm(range(len(config_sizes))):
            input_size = config_sizes[i]
            print(f"running model for size {input_size}")
            time = run_the_processes_for_c(size=input_size)
            print(f"model finished for size {input_size}")
            size_data.append(input_size)
            c_time_data.append(time)
            time = run_the_processes_for_python(size=input_size)
            python_time_data.append(time)

        plt.plot(size_data, c_time_data, color="red")
        plt.plot(size_data, python_time_data, color="blue")
        plt.show()
