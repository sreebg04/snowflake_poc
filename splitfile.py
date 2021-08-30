import os
from os import listdir
from os.path import isfile, join, isdir
from pathlib import Path
import concurrent.futures


def split_all_files(source):
    linesPerFile = 1000000
    filename = 1
    resultfiles = []
    newfolder = join(os.path.dirname(source), Path(source).stem)
    if not os.path.exists(newfolder):
        os.makedirs(newfolder)
    with open(source, 'r') as f:
        csvfile = f.readlines()
    for i in range(0, len(csvfile), linesPerFile):
        with open((join(newfolder, (str(Path(source).stem) + "_" + str(filename)) + '.csv')), 'w+') as f:
            if filename > 1:
                f.write(csvfile[0])
            f.writelines(csvfile[i:i + linesPerFile])
        filename += 1
    onlyfiles = [f for f in listdir(newfolder) if isfile(join(newfolder, f))]
    for file in onlyfiles:
        resultfiles.append(join(newfolder, file))
    return resultfiles


def split(source):
    file_list = []
    result_list = []
    for direc in listdir(source):
        if isdir(join(source, direc)):
            for item in listdir(join(source, direc)):
                fileitem = join(source, direc, item)
                file_list.append(fileitem)

    for file in file_list:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(split_all_files, file)
            return_value = future.result()
            result_list.append(return_value)
    finallist = [item for sublist in result_list for item in sublist]
    return finallist
