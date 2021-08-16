import csv
import json
import os

import pandas as pd
import pymongo

from config import MongoDb


def csv_to_json(excel_file_path, csv_file_path, json_file_path, csv_tmp_file_path):
    def create_tmp_csv_file(csv_tmp_file_path, excel_file_to_csv):
        print("creating temporary file with modified header")
        col = csv.writer(open(csv_tmp_file_path,
                              'w',
                              newline=""))
        for row in excel_file_to_csv:
            col.writerow(row)

    print("convert xlsx to csv")
    excel_file_to_csv = pd.read_excel(excel_file_path, engine="openpyxl")
    excel_file_to_csv.to_csv(csv_file_path, index=None, header=True)

    print("opening csv file")
    file = open(csv_file_path)
    csv_file = list(csv.reader(file))

    print("converting first row to lower")
    headers = csv_file[0]
    header_lowered = list()
    for header in headers:
        header_lowered.append(header.lower())
    csv_file.remove(headers)
    csv_file.insert(0, header_lowered)

    create_tmp_csv_file(csv_tmp_file_path, csv_file)

    def remove_tmp_file(csv_tmp_file_path):
        print("Removing temporary file")
        if os.path.exists(csv_tmp_file_path):
            os.remove(csv_tmp_file_path)

    json_array = []

    print("read csv file")
    with open(csv_tmp_file_path, encoding='cp1252') as csvf:
        print("load csv file data using csv library's dictionary reader")

        csv_reader = csv.DictReader(csvf)
        print("convert each csv row into python dict and add to json_array")
        for row in csv_reader:
            json_array.append(row)

    print("convert python json_array to JSON String and write to file")
    if not os.path.exists(json_file_path):
        open(json_file_path, "x")
    with open(json_file_path, 'w', encoding='cp1252') as jsonf:
        json_string = json.dumps(json_array, indent=4)
        jsonf.write(json_string)

    remove_tmp_file(csv_tmp_file_path)


def insert_mongo(json_file):
    print("create connection string")

    user = MongoDb["username"]
    password = MongoDb["password"]

    client = pymongo.MongoClient(MongoDb["connection"].format(user, password))
    db = client[MongoDb["database"]]
    col = db[MongoDb["collection"]]

    print("insert json file on mongodb")

    with open(json_file) as f:
        file_data = json.load(f)
    col.insert_many(file_data)

    client.close()

    print("end of process")
