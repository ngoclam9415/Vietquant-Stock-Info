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

    def creating_insert_datas(self, data_type:"String", dict_data : "Dictionary"):
        insert_datas = []
        for key, value in dict_data.items():
            data = {}
            data["item_type"] = data_type
            data["item_name"] = key
            data["inserted_time"] = datetime.fromtimestamp(float(value.split("|")[1])/1000) if data_type == "STOCK" else datetime.now()
            data["item_value"] = float(value.split("||||||")[0].split("|||")[-1].split("|")[1])
            insert_datas.append(data)
        return insert_datas

if __name__ == "__main__":
    db = StockDBAccessor("localhost", 27017)
    db.insert_single_item("Hehe", "Oke", 1234, 1579592821958/1000)
    