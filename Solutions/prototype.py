import os
import random
import sqlite3
import names
import shutil
import threading
import tqdm
import pandas as pd
from queue import Queue
from pathlib import Path


# sorting csvs function
def func(elem):
    return int(str(elem).split('myCSV')[-1].split('.')[0])


def find_files_to_merge(csv_dict: dict, threshold: float = 64.0):
    """
    Description
    -----------
    gets a dictionary of file and its size that contains multiple "small" files and outputs a list containing small
    lists of files that needs to be merged in order for the file's sized to be large enough (64 by default)

    Args
    ----
    csv_dict (dict) = dictionary of the following structure: {file_name: (file_size, pd.read_csv(file))}
    threshold (float) = desired threshold for a file to be large enough

    Returns
    -------
    main list that contains sub-lists of files that need to be merged (i.e if the main list's length is 3 then there
    are 3 sub-lists such that each sub-list contains files that need to be merged)
    """
    large_files_list = []
    cur_files_list = []
    cur_chunk = 0
    for value in csv_dict.values():
        if (value[0] + cur_chunk) / 1000000 < threshold:
            cur_chunk += value[0]
            cur_files_list.append(value[1])
        else:
            cur_files_list.append(value[1])
            large_files_list.append(cur_files_list)
            cur_files_list = []
            cur_chunk = 0
    return large_files_list


# creating the decorator
def ResultsQueue(func):
    def wrapper(*args):
        queue.put(func(*args))

    return wrapper


@ResultsQueue
def inverted_map(document: Path):
    """
    This function reads the CSV document from the local disc
    and return a list that contains entries of the form (key_value, document name)
    """
    # read the csv and create an empty list
    document_name = document.name
    data = pd.read_csv(document)
    inverted_list = []

    # convert each row in the csv to a dictionary and add it to the list
    for row in tqdm.tqdm(data.iterrows(), total=data.shape[0], desc=f'processing map function {document_name}'):
        inverted_list.append([(f"{key}_{value}", document_name) for key, value in row[1].items()])
    return inverted_list


@ResultsQueue
def inverted_reduce(value, documents):
    """
    The field “documents” contains a list of all CSV documents per given value.
    This list might have duplicates.
    The function will return new list without duplicates.
    """

    # convert the long string of csvs to a list
    documents = documents.split(',')

    # return a list without duplicates
    return [value] + list(set(documents))


class MapReduceEngine():

    def execute(self, input_data, map_function, reduce_function):

        # For each key from the input_data, start a new Python thread that executes map_function(key)
        map_threads = []
        for key in input_data:
            t = threading.Thread(target=map_function, args=[key, ])
            t.start()
            map_threads.append(t)

        # Each thread will store results of the map_function into mapreducetemp/part-tmp-X.csv
        # X is a unique number per each thread
        for idx, thread in enumerate(map_threads):
            thread.join()
            results = queue.get()
            df = pd.DataFrame([item for sublist in
                               tqdm.tqdm(results, desc=f'creating temp files {idx + 1}/{len(map_threads)}')
                               for item in sublist], columns=['key', 'value'])
            df.to_csv(f'{folder_path}/mapreducetemp/part-tmp-{idx + 1}.csv', index=False)

        # Keep the list of all threads and check whether they are completed
        for t in map_threads:
            if not t.is_alive():
                # get results from thread
                t.handled = True
        map_threads_validate = [t for t in map_threads if t.handled]  # the list of all valid threads

        # check if whether all of the threads are completed
        if len(map_threads) == len(map_threads_validate):
            print('Map treads completed succesfully')
            completed = True
        else:
            completed = False
            print('Map treads failed!!')
            return completed

        # Once all threads completed, load content of all CSV files into the temp_results table in SQLite
        if completed:
            for file in tqdm.tqdm((folder_path/'mapreducetemp').rglob('*.csv'), desc='loading data to sql'):
                df = pd.read_csv(file)
                df.to_sql('temp_results', connection, if_exists='append', index=False)

            # Write SQL statement that generates a sorted list by key of the form (key, value)
            # where value is concatenation of ALL values in the value column that match specific key
            cursor.execute('''SELECT Key, GROUP_CONCAT(value) FROM temp_results GROUP BY Key ORDER BY Key''')
            key_value_list = cursor.fetchall()

            # Start a new thread for each value from the generated list in the previous step,
            # to execute reduce_function(key,value)
            reduce_threads = []
            for value in key_value_list:
                t = threading.Thread(target=reduce_function, args=[value[0], value[1], ])
                t.start()
                reduce_threads.append(t)

            # Each thread will store results of reduce_function into mapreducefinal/part-X-final.csv file
            for idx, thread in enumerate(reduce_threads):
                thread.join()
                results = queue.get()
                df = pd.DataFrame(results[1:], columns=[results[0]])
                df.to_csv(f'{folder_path}/mapreducefinal/part-{idx + 1}-final.csv', index=False)

            # Keep list of all threads and check whether they are completed
            for t in reduce_threads:
                if not t.is_alive():
                    # get results from thread
                    t.handled = True
            reduce_threads_validate = [t for t in reduce_threads if t.handled]  # the list of all valid threads

            # Once all threads completed, print on the screen MapReduce Completed otherwise print MapReduce Failed
            if len(reduce_threads) == len(reduce_threads_validate):
                print('Reduce treads completed succesfully')
                return '\nMapReduce Completed'
            else:
                return '\nMapReduce Failed'


if __name__ == '__main__':

    # define folder path in which the data will be saved
    cur_dir = Path.cwd()
    folder_path = cur_dir / 'outputs'
    if os.path.isdir(folder_path):
        shutil.rmtree(folder_path)
    os.mkdir(folder_path)

    # define 'queue' variable which will be used to return the values of the functions in the threads
    queue = Queue()

    firstname = ['John', 'Dana', 'Scott', 'Marc', 'Steven', 'Michael', 'Albert', 'Johanna']
    city = ['NewYork', 'Haifa', 'Munchen', 'London', 'PaloAlto', 'TelAviv', 'Kiel', 'Hamburg']

    # using 'names' library we could easily generate random second names
    secondname = []
    for i in range(len(firstname)):
        random_name = names.get_first_name()
        secondname.append(random_name)

    # let's create the 20 csvs
    for i in tqdm.tqdm(range(1, 97), desc='creating csvs'):
        firstname_col = random.choices(firstname, k=100000)
        secondname_col = random.choices(secondname, k=100000)
        city_col = random.choices(city, k=100000)
        df = pd.DataFrame(list(zip(firstname_col, secondname_col, city_col)),
                          columns=['firstname', 'secondname', 'city'])
        df.to_csv(f'{str(folder_path)}/myCSV{i}.csv', index=False)

    # check if the folder exist, if so then remove it and create a new one
    if os.path.exists(f'{str(folder_path)}/mapreducetemp'):
        shutil.rmtree(f'{str(folder_path)}/mapreducetemp')
    os.mkdir(f'{str(folder_path)}/mapreducetemp')

    if os.path.exists(f'{str(folder_path)}/mapreducefinal'):
        shutil.rmtree(f'{str(folder_path)}/mapreducefinal')
    os.mkdir(f'{str(folder_path)}/mapreducefinal')

    connection = sqlite3.connect(f'{str(folder_path)}/hw2.db')
    cursor = connection.cursor()

    cursor.execute('''CREATE TABLE IF NOT EXISTS temp_results(
                        key text,
                        value text)''')

    csvs_dict = {}
    for file in sorted(folder_path.rglob('*csv'), key=func):
        csvs_dict[file.name] = (file.stat().st_size, pd.read_csv(file))

    files_to_merge = find_files_to_merge(csvs_dict)

    for idx, files_list in enumerate(tqdm.tqdm(files_to_merge, desc='merging csvs')):
        merged_file = pd.concat(files_list)
        merged_file.to_csv(folder_path / f'mergedCSV{idx + 1}.csv', index=False)

    for i in folder_path.rglob('myCSV*'):
        os.remove(i)

    input_data = [csv for csv in folder_path.rglob('*.csv')]

    mapreduce = MapReduceEngine()
    status = mapreduce.execute(input_data, inverted_map, inverted_reduce)
    print(status)

    # if the folder exists, delete it recursively
    if os.path.exists(folder_path / 'mapreducetemp'):
        shutil.rmtree(folder_path / 'mapreducetemp')

    # try to close the connection
    try:
        cursor.close()
        connection.close()
    # if the exception is 'ProgrammingError' then the connection is already close
    # print it
    except sqlite3.ProgrammingError:
        print('Connection is already closed!')

    # if the database exists - remove it
    if os.path.exists(folder_path / 'hw2.db'):
        os.remove(folder_path / 'hw2.db')
