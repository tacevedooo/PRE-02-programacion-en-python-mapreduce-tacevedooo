"""Taller evaluable"""

# pylint: disable=broad-exception-raised

import fileinput
import glob
import os.path
import string
import time
from itertools import groupby



def copy_raw_files_to_input_folder(n):
    """Generate n copies of the raw files in the input folder"""
    create_directory("files/input")
    for file in glob.glob("files/raw/*"):
        with open(file,"r",encoding="utf-8") as f:
            text=f.read()
        for i in range(1,n+1):
            filename=f"{os.path.basename(file).split('.')[0]}_{i}.txt"
            with open(f"files/input/{filename}","w",encoding="utf-8") as f2:
                f2.write(text)


def load_input(input_directory):
    """Funcion load_input"""
    sequence=[]
    files=glob.glob(f"{input_directory}/*")
    with fileinput.input(files=files)as f:
        for line in f:
            sequence.append((fileinput.filename(),line))
    return sequence

def preprocess_line(x):
    """Preprocess the line x"""
    text=x[1]
    text=text.lower()
    text=text.translate(str.maketrans("","",string.punctuation))
    text=text.replace("\n","")
    return(x[0],text)


def map_line(x):#//
    x=preprocess_line(x)
    x=x[1].split()#//toma la frase y devuelve []con cada palabra
    return [(w,1)for w in x] #//make tupple word,1


def mapper(sequence):
    """Mapper"""
    sequence=[pair for sublist in map(map_line,sequence) for pair in sublist]
    return sequence


def shuffle_and_sort(sequence):
    """Shuffle and Sort"""
    sequence.sort(reverse=False, key=lambda x:x[0])
    return sequence
    


def compute_sum_by_group(group):
    pass

def reducer(sequence):
    """Reducer"""
    result=[]
    for key,value in sequence:
        if result and result[-1][0]==key:
            result[-1]=(key,result[-1][1]+value)
        else:
            result.append((key,value))
    return result


def create_directory(directory):
    """Create Output Directory"""
    if os.path.exists(directory):
        for file in glob.glob(f"{directory}/*"):
            os.remove(file)
        os.rmdir(directory)
    os.makedirs(directory)

def save_output(output_directory, sequence):
    """Save Output"""
    file=f"{output_directory}/part-00000"
    with open(file,"w",encoding="utf-8") as f:
        for k,v in sequence:
            f.write(f"{k}\t {v}\n")
    f.close()

def create_marker(output_directory):
    """Create Marker"""
    file=f"{output_directory}/_SUCCESS"
    with open(file,"w",encoding="utf-8") as f:
        pass
    f.close()


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