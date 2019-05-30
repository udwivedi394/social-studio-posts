import json
import os
import shutil
from collections import defaultdict
from genericpath import isfile, isdir
from os import listdir
from os.path import join, basename, normpath
from shutil import rmtree, copyfile
from typing import List


class OSFileOperations:
    @staticmethod
    def get_all_files(path, recursive=True):
        files = []
        for file_name in listdir(path):
            file_path = join(path, file_name)
            if isfile(file_path):
                files.append(file_path)
            elif isdir(file_path):
                if recursive:
                    files += OSFileOperations.get_all_files(file_path)
        return files

    @staticmethod
    def get_all_csv_files(path, recursive=True):
        files = OSFileOperations.get_all_files(path, recursive=recursive)
        files = [file for file in files if file.endswith("csv")]
        return files

    @staticmethod
    def get_all_dirs(path, recursive=True):
        dirs = []
        for dir_path in listdir(path):
            dir_path = join(path, dir_path)
            if isfile(dir_path):
                continue
            elif isdir(dir_path):
                dirs.append(dir_path)
                if recursive:
                    dirs += OSFileOperations.get_all_dirs(dir_path)
        return dirs

    @staticmethod
    def get_base_name(path):
        base_name = basename(normpath(path))
        return base_name

    @staticmethod
    def ensure_directory(file_path):
        directory = OSFileOperations.get_dir_path(file_path)
        if not directory:
            # file_path is a file, no need to create directory.
            return
        if not OSFileOperations.entity_exists(directory):
            os.makedirs(directory)

    @staticmethod
    def remove_entity(entity_path):
        if not OSFileOperations.entity_exists(entity_path):
            return
        if isfile(entity_path):
            os.remove(entity_path)
        else:
            rmtree(entity_path)

    @staticmethod
    def entity_exists(file_path):
        return os.path.exists(file_path)

    @staticmethod
    def get_all_sub_directories(directory_path: str) -> List[str]:
        sub_directories = list()
        files_and_folder_names = os.listdir(directory_path)
        for item in files_and_folder_names:
            path = '{}{}'.format(directory_path, item)
            if os.path.isdir(path):
                sub_directories.append(item)
        return sub_directories

    @staticmethod
    def copy_file(source_file_path, destination):
        OSFileOperations.ensure_directory(destination)
        copyfile(source_file_path, destination)

    @staticmethod
    def get_dir_path(file_path):
        directory = os.path.dirname(file_path)
        return directory

    @staticmethod
    def copy_directory(source, destination, overwrite=False):
        if overwrite:
            OSFileOperations.remove_entity(destination)
        shutil.copytree(source, destination)

    @staticmethod
    def move_directory(source, destination, overwrite=False):
        if overwrite:
            OSFileOperations.remove_entity(destination)
        OSFileOperations.copy_directory(source, destination)
        OSFileOperations.remove_entity(source)

    @staticmethod
    def rename_entity(source, destination):
        if not OSFileOperations.entity_exists(source):
            return
        os.rename(source, destination)

    @staticmethod
    def remove_directory(path):
        shutil.rmtree(path)


class JSONUtils:
    @staticmethod
    def write_json_data_to_file(file, data_dict):
        OSFileOperations.ensure_directory(file)
        with open(file, 'w') as outfile:
            json.dump(data_dict, outfile)

    @staticmethod
    def load_json_data_from_file(input_file, default_dict=False, defaultdict_type=dict):
        with open(input_file) as file:
            data_dict = json.load(file)
            if default_dict:
                return defaultdict(defaultdict_type, data_dict)
            return data_dict
