"""Taller evaluable"""

# pylint: disable=broad-exception-raised

import fileinput
import glob
import os.path
import time
import shutil 
import string
from itertools import groupby

from toolz.itertoolz import concat, pluck



def copy_raw_files_to_input_folder(n):
    """Generate n copies of the raw files in the input folder"""
    raw_folder = "files/raw"
    input_folder = "files/input"
    os.makedirs(input_folder, exist_ok=True)
    raw_files = glob.glob(os.path.join(raw_folder, "*"))
    if not raw_files:
        raise Exception(f"No raw files foudn in  'files/raw'")
    for i in range(n):
        for f in raw_files:
            shutil.copy(f, os.path.join(input_folder, f"copy_{i}_{os.path.basename(f)}"))    
            

def load_input(input_directory):
    """Funcion load_input"""
    files = glob.glob(os.path.join(input_directory, "*"))
    return fileinput.input(files, openhook=fileinput.hook_encoded("utf-8"))


def preprocess_line(x):
    """Preprocess the line x"""
    return x.lower().translate(str.maketrans("", "", string.punctuation)).strip()


def map_line(x):
    """Map the line to (word, 1) pairs"""
    return [(word, 1) for word in preprocess_line(x).split() if word]


def mapper(sequence):
    """Mapper"""
    return concat(map(map_line, sequence))


def shuffle_and_sort(sequence):
    """Shuffle and Sort"""
    sorted_seq = sorted(sequence, key=lambda x: x[0])
    for key, group in groupby(sorted_seq, key=lambda x: x[0]):
        yield (key, list(pluck(1, group)))


def compute_sum_by_group(group):
    """ Compute sum for group of values"""
    key, values = group
    return (key, sum(values))


def reducer(sequence):
    """Reducer"""
    return map(compute_sum_by_group, sequence)


def create_directory(directory):
    """Create Output Directory"""
    os.makedirs(directory, exist_ok=True)


def save_output(output_directory, sequence):
    """Save Output"""
    output_file = os.path.join(output_directory, "part-00000")
    with open(output_file, "w", encoding="utf-8") as f:
        for key, value in sequence:
            f.write(f"{key}\t{value}\n")


def create_marker(output_directory):
    """Create Marker"""
    with open(os.path.join(output_directory, "_SUCCESS"), "w", encoding="utf-8") as f:
        f.write("Job completed successfully\n")


def run_job(input_directory, output_directory):
    """Job"""
    sequence = load_input(input_directory)
    sequence = mapper(sequence)
    sequence = shuffle_and_sort(sequence)
    sequence = reducer(sequence)
    create_directory(output_directory)
    save_output(output_directory, sequence)
    create_marker(output_directory)


if __name__ == "__main__":

    copy_raw_files_to_input_folder(n=1000)

    start_time = time.time()

    run_job(
        "files/input",
        "files/output",
    )

    end_time = time.time()
    print(f"Tiempo de ejecución: {end_time - start_time:.2f} segundos")
