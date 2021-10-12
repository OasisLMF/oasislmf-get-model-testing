import json
import os
import subprocess
from typing import List, Tuple
from tqdm import tqdm
from pathlib import Path


import matplotlib.pyplot as plt

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
    return str(os.getcwd()) + "/data/" + str(size) + "_config.json"


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
    config_sizes = [100, 500, 600, 700, 800, 900, 1000]

    write_configs(sizes=config_sizes)

    size_data = []
    time_data = []

    for i in tqdm(range(len(config_sizes))):
        input_size = config_sizes[i]
        print(f"preparing data for size {input_size}")
        generate_data(size=input_size)
    #     print(f"running model for size {input_size}")
    #     time = run_the_processes_for_c(size=input_size)
    #     print(f"model finished for size {input_size}")
    #     size_data.append(input_size)
    #     time_data.append(time)
    #
    # plt.plot(size_data, time_data)
    # plt.show()
