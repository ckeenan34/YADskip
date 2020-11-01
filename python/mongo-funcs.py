from pymongo import MongoClient

client = MongoClient('mongodb://admin:<password here>@192.168.0.18:27017')
db=client.admin

# serverStatusResult=db.command("serverStatus")

# print(serverStatusResult)

# def add_channel()