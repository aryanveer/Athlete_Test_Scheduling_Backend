import pymongo
from random import randint

CONNECTION_STRING = "mongodb://group6api:YXeNbRRXMlCbDuHPyCG10sQARbUllgR4tmazMeDu946ZagcBAWlHu9qAXCoRrjOiuFCaR8glsmYr5CM1v42RLg==@group6api.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@group6api@"
DB_NAME = "EuropeDB"
READ_COLLECTION_NAME = "ReadCollection"
WRITE_COLLECTION_NAME = "WriteCollection"
SAMPLE_FIELD_NAME = "test"

def delete_document(collection, document_id):
    """Delete the document containing document_id from the collection"""
    collection.delete_one({"_id": document_id})
    print("Deleted document with _id {}".format(document_id))

def read_document(collection, document_id):
    """Return the contents of the document containing document_id"""
    print("Found a document with _id {}: {}".format(document_id, collection.find_one({"_id": document_id})))

def update_document(collection, document_id):
    """Update the sample field value in the document containing document_id"""
    collection.update_one({"_id": document_id}, {"$set":{SAMPLE_FIELD_NAME: "Updated!"}})
    print("Updated document with _id {}: {}".format(document_id, collection.find_one({"_id": document_id})))

def insert_sample_document(collection):
    """Insert a sample document and return the contents of its _id field"""
    document_id = collection.insert_one({SAMPLE_FIELD_NAME: randint(50, 500)}).inserted_id
    print("Inserted document with _id {}".format(document_id))
    return document_id

def create_sharded_collection(client):
    """Create sample database with shared throughput if it doesn't exist and an unsharded collection"""
    db = client[DB_NAME]

    # Create database if it doesn't exist
    if DB_NAME not in client.list_database_names():
        # Database with 400 RU throughput that can be shared across the DB's collections
        db.command({'customAction': "CreateDatabase", 'offerThroughput': 400, 'shardKey': "/location_id" })
        print("Created db {} with shared throughput". format(DB_NAME))
    
    # Create collection if it doesn't exist
    if READ_COLLECTION_NAME not in db.list_collection_names():
        # Creates a unsharded collection that uses the DBs shared throughput
        db.command({'customAction': "CreateCollection", 'collection': READ_COLLECTION_NAME})
        print("Created collection {}". format(READ_COLLECTION_NAME))

    return db.READ_COLLECTION_NAME

def main():
    """Connect to the API for MongoDB, create DB and collection, perform CRUD operations"""
    client = pymongo.MongoClient(CONNECTION_STRING)
    try:
        client.server_info() # validate connection string
    except pymongo.errors.ServerSelectionTimeoutError:
        raise TimeoutError("Invalid API for MongoDB connection string or timed out when attempting to connect")

    collection = create_sharded_collection(client)
    
    document_id = insert_sample_document(collection)
   # print(document_id)
   # read_document(collection, document_id)
   # update_document(collection, document_id)
   ## delete_document(collection, document_id)


if __name__ == '__main__':
    main()