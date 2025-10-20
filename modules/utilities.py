import modules.constants as const
import json
import re

def add_new_column_mapper(mapper_name: str, mapper_values:dict):
    """
    Opens the mapper
    Appends a new columns mapper
    Saves the mapper with the newly added dictionary
    """
    # open mapper
    file_path = const.FILE_COLUMNS_DICT_PATH
    with open(file_path, 'r') as file:
        mappers_dict = json.load(file)
        
    # append new values
    mappers = mappers_dict.get('mappers')
    new_entry = {
        'name': mapper_name,
        'map': mapper_values
    }
    mappers.append(new_entry)
    mappers_dict.update({'mappers':mappers})

    # save mapper
    with open(file_path, 'w') as file:
        json.dump(mappers_dict, file, indent=4)

# CHECK: include words boundaries to regex pattern
def find_matching_columns(keywords:list, current_column_names:list) -> list:
    keywords = [x.lower() for x in keywords]
    pattern = '|'.join(keywords)
    pattern = re.compile(pattern)
    matching_columns = [x for x in current_column_names if re.search(pattern,x)]

    return matching_columns

def create_new_column_mapper(current_column_names:list) -> dict:
    """
    logic to input the key (current column name) and the value (new column name)
    returns the mapper as a dictionary
    """

    column_mapper = {}
    for current_column_name in current_column_names:
        print(current_column_name)

        # PENDING - prints suggested columns (regex pattern using each word as keyword)
        keywords = current_column_name.split(' ')
        matching_columns = find_matching_columns(keywords, const.DB_COLUMNS)
        print('suggested matching columns: {}'.format(matching_columns))
        print('all columns: {}'.format(const.DB_COLUMNS))

        new_column_name = input('new column name: ')
        column_mapper.update({current_column_name:new_column_name})
        print('')

    return column_mapper