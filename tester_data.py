from random import randint, random
import pymongo

CONNECTION_STRING = "mongodb://group6api:YXeNbRRXMlCbDuHPyCG10sQARbUllgR4tmazMeDu946ZagcBAWlHu9qAXCoRrjOiuFCaR8glsmYr5CM1v42RLg==@group6api.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@group6api@"
DB_NAME = "EuropeDB"

def populate_tester_collection():

    db = client = pymongo.MongoClient(CONNECTION_STRING)
    db = client[DB_NAME]

    collection_prefix = ["NA", "EU", "AU", "AS"]
    regions = ["North America", "Europe", "Australia", "Asia"]
    countries = ["America", "Ireland", "Australia", "China"]
    random_names = ["conor", "ankana", "yuki", "aryan", "sedat", "john", "mark", "paul"]

    i = 0
    while i < 50: 

        random_num = randint(0,3)
        random_name = randint(0,7)
        data = {"tester_email": random_names[random_name]+str(random_num)+"@gmail.com", "region": regions[random_num], "country": countries[random_num]}
        db[collection_prefix[random_num]+"-testers"].insert_one(data).inserted_id
        i+=1



def main():
    populate_tester_collection()


if __name__ == '__main__':
    main()
