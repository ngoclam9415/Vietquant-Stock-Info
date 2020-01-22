from pymongo import MongoClient
from datetime import datetime

class StockDBAccessor:
    def __init__(self, ip:"String", port:"Integer"):
        self.mongodb = MongoClient(host=ip, port=port)
        self.stock_db = self.mongodb["Stock"]
        self.stock_collection = self.stock_db["Stock_info"]

    def insert_single_item(self, item_type:" String", item_name:"String", item_value:"Float", time_stamp: "second float"):
        insert_data = {"item_type" : item_type,
                        "item_name" : item_name,
                        "item_value" : item_value,
                        "inserted_time" : datetime.fromtimestamp(time_stamp)}
        self.stock_collection.update_one(insert_data, {"$set" : insert_data}, upsert=True)

    def insert_many_items(self, items : "List"):
        self.stock_collection.insert_many(items)

    def creating_insert_datas(self, dict_data : "Dictionary"):
        insert_datas = []
        # print(dict_data)
        if dict_data["type"] == "returnData":
            for key, value in dict_data["data"]["data"].items():
                data = {}
                data["item_type"] = dict_data["data"]["name"]
                data["item_name"] = key
                data["inserted_time"] = datetime.fromtimestamp(float(value.split("|")[1])/1000) if data["item_type"] == "STOCK" else \
                        datetime.strptime("{}-{}-{} {}".format(datetime.now().year, datetime.now().month, datetime.now().day, value.split("|")[-4]), "%Y-%m-%d %H:%M:%S")
                # data["item_value"] = float(value.split("||||||")[0].split("|||")[-1].split("|")[1]) if data["item_type"] == "STOCK" else value.split("|||")[1].split("|")[0]
                data["item_value"] = float(value.split("|")[20] if data["item_type"] == "STOCK" else value.split("|")[14])
                data["raw_data"] = value
                insert_datas.append(data)
        elif dict_data["type"] != "info":
            data = {}
            data["item_type"] = dict_data["type"]
            value = dict_data["data"]
            data["item_name"] = value.split("|")[3] if data["item_type"] == "STOCK" else value.split("|||")[0].split("|")[-1]
            data["inserted_time"] = datetime.fromtimestamp(float(value.split("|")[1])/1000) if dict_data["type"] == "STOCK" else \
                        datetime.strptime("{}-{}-{} {}".format(datetime.now().year, datetime.now().month, datetime.now().day, value.split("|")[-4]), "%Y-%m-%d %H:%M:%S")
            data["item_value"] = float(value.split("|")[20] if data["item_type"] == "STOCK" else value.split("|")[14])
            # data["item_value"] = float(value.split("||||||")[0].split("|||")[-1].split("|")[1]) if data["item_type"] == "STOCK" else value.split("|||")[1].split("|")[0]
            data["raw_data"] = value
            insert_datas.append(data)
        return insert_datas

if __name__ == "__main__":
    # db = StockDBAccessor("localhost", 27017)
    # print(datetime.strptime("{}-{}-{} 09:09:28".format(datetime.now().year, datetime.now().month, datetime.now().day), "%Y-%m-%d %H:%M:%S"))
    print(datetime.now().minute)