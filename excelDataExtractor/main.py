import importMongo
from config import Path


def init_process():
    print("Start process to import mongo documents from csv")

    try:
        csv_file_path = Path["csv_file_path"]
        tmp_file_path_array = csv_file_path.split(".")
        csv_tmp_file_path = rf'{tmp_file_path_array[0]}tmp.{tmp_file_path_array[1]}'
        importMongo.csv_to_json(Path["excel_file_path"], Path["csv_file_path"], Path["json_file_path"],
                                csv_tmp_file_path)

    except Exception as ex:
        print("Error converting csv to json", ex)

    print("Start process to import mongo documents from json")

    try:
        importMongo.insert_mongo(Path["json_file_path"])

    except Exception as ex:
        print("Error inserting json file", ex)


if __name__ == "__main__":
    init_process()
