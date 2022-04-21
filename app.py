import json
import os
import random
import requests
import datetime
from textwrap import indent
import time
from urllib import response
from flask import Flask, jsonify, request, make_response
from bson.json_util import dumps
from flask_cors import CORS
from pymongo import MongoClient
from collections import defaultdict
import uuid

from db_commands import CONNECTION_STRING # CONOR's string
from helper_country import countries_to_continent

app = Flask(__name__)
cors = CORS(app, resources={r"/*": {"origins": "*"}})

client = MongoClient(CONNECTION_STRING)
db = client['EuropeDB']  

time_obj = datetime.datetime

region_to_code = {
    "Europe" : "EU",
    "North America" : "NA",
    "Asia": "AS",
    "Australia" : "AU"
}

continent_to_countries = {'EU' : ['France', 'Ireland', 'England'],
                          'NA' : ['Canada', 'America'],
                          'AS' : ['China', 'India', 'Japan'],
                          'AU' : ['Australia']}



@app.route('/')
def root():
    persist = request.args.get('blah')
    # location = os.environ["APP_LOCATION"]
    if(persist):
        print("Hi")
    return make_response(jsonify("Working"), 200)


# creating availability endpoint. This is where the user will 'POST'
# request to create availability. The request will have a list of availabilities
# NOT a single availability, below is the request we get:
#
# {athlete_email : 'sedat@gmail.com',
#  availabilities: [{
#           athlete_email: 'sedat@gmail.com',
#           region: 'Europe',
#           country: 'Ireland',
#           location: 'SLS'
#           city: 'Dublin',
#           date: 24/04/2022
#           time: 16:00:00, (GMT)  -----> if not, it gets too complicated. Assume everyone considers GMT
#           timestamp: 3434334343, (GMT)
#           
#           
#          },
#          {
#           athlete_email: 'sedat@gmail.com',
#           region: 'Europe',
#           country: 'Ireland',
#           location: 'SLS'
#           city: 'Dublin',
#           date: 24/04/2022
#           time: 16:00:00, (GMT)
#           timestamp: 3434334343 (GMT)
#          }]     
# 
# }

# @app.route('/testQueue', methods=['GET'])
# def export_queue():
#     CONNECTION_STR_QUEUE = 'Endpoint=sb://group6.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=5kneXNBlqHf8oUVOO5EC5sdVOS+aTOqygeJbj2Dq3aQ='
#     QUEUE_NAME = 'group6availability'
#     servicebus_client = ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR_QUEUE, logging_enable=True)
#     with servicebus_client:
#         receiver = servicebus_client.get_queue_receiver(queue_name=QUEUE_NAME, max_wait_time=5)
#         with receiver:
#             for msg in receiver:
#                 print("Received: " + str(json.loads(str(msg))))
#                 receiver.complete_message(msg)
#                 availability_data = json.loads(str(msg))
#                 print(availability_data)


@app.route('/createAvailability', methods=['POST'])
def create_availability():
    availabilities_response = []
    availability_data = request.get_json()
    persist = request.args.get('persist')
    commit = request.args.get('commit')
    rollback = request.args.get('rollback')
    location = os.environ["APP_LOCATION"]
    region_code = region_to_code[location]

    if(persist):
        db_addition = db[region_code + "-persist"].insert_one(availability_data).inserted_id
        if(db_addition):
            return make_response(jsonify(json.loads(dumps(availabilities_response, indent=10))), 200)
        else:
            response["status"] = "Failed to add data to persist collection in " + region_code
            return make_response(jsonify(response), 406)

    if(commit):
        uuid_persisted = availability_data['uuid']
        myquery = { "uuid": uuid_persisted }
        persisted_in_db = db[region_code + "-persist"].find_one(myquery)
        success = add_availabilities_in_db(region_code, availabilities_response, persisted_in_db)
        return make_response(jsonify(json.loads(dumps(availabilities_response, indent=10))), 200)

    if(rollback):
        delete_persisted_data(region_code, availability_data['uuid'])

    athlete_email = availability_data['athlete_email']
    availabilities = availability_data['availabilities']

    checks_response, checks_status = check_validity_for_availabilities(availabilities)
    if(checks_status is False):
        return make_response(jsonify(checks_response), 406)

    regions_availabilities_dict = {'North America':{'athlete_email':athlete_email, 'availabilities':[]},
                                     'Europe':{'athlete_email':athlete_email, 'availabilities':[]}, 
                                    'Australia':{'athlete_email':athlete_email, 'availabilities':[]},
                                     'Asia':{'athlete_email':athlete_email, 'availabilities':[]}}

    for a in availabilities:
        regions_availabilities_dict[a['region']]['availabilities'].append(a)

    url = "https://internalLoadBalancerFrontDoor.azurefd.net/createAvailability"
    
    total_count = set()
    success_count = set()

    # Transactions persist phase
    uuid_for_persist = str(uuid.uuid4())
    for key in regions_availabilities_dict.keys():
        if key != location:
            if(len(regions_availabilities_dict[key]['availabilities']) > 0):
                total_count.add(key)
                headers = {"x-preferred-backend": key} 
                params = {"persist":True}
                data = regions_availabilities_dict[key]
                data['uuid'] = uuid_for_persist
                response = requests.post(url, params=params, headers=headers, json=data)
                if(response.status_code != 200):
                    availabilities_response.append("Not persisted: " + key)
                else:
                    success_count.add(key)
        else:
            if(len(regions_availabilities_dict[key]['availabilities']) > 0):
                total_count.add(key)
                data = regions_availabilities_dict[key]
                data['uuid'] = uuid_for_persist
                db_addition = db[region_code + "-persist"].insert_one(data).inserted_id
                if(db_addition):
                    success_count.add(key)
                else:
                    availabilities_response.append("Not persisted: " + key)

    # commit phase
    if(total_count == success_count):
        for key in success_count:
            if key != location:
                headers = {"x-preferred-backend": key}
                params = {"commit":True}
                data = {"uuid":uuid_for_persist}
                response = requests.post(url,  params=params, headers=headers, json=data)
                if(response.status_code != 200):
                    availabilities_response.append("Not nice: " + key)
                else:
                    availabilities_response.append(response.json())
            else:
                myquery = { "uuid": uuid_for_persist }
                persisted = db[region_code + "-persist"].find_one(myquery)
                add_availabilities_in_db(region_code, availabilities_response, persisted)
    else:
        for key in success_count:
            if key != location:
                headers = {"x-preferred-backend": key}
                params = {"rollback":True}
                data = {"uuid":uuid_for_persist}
                response = requests.post(url,  params=params, headers=headers, json=data)
                if(response.status_code != 200):
                    availabilities_response.append("Not nice: " + key)
                else:
                    availabilities_response.append(response.json())
            else:
                delete_persisted_data(region_code, uuid_for_persist)

    return make_response(jsonify(json.loads(dumps(availabilities_response, indent=10))), 200)


def delete_persisted_data(code, persisted_uuid):
    db[code + "-persist"].delete_one({ "$and": {"uuid": {"$eq": persisted_uuid}} })


def check_validity_for_availabilities(data):
    time_now = time_obj.now().timestamp()
    for athlete_availability in data:
        date = athlete_availability['date']  # Day/Month/Year
        daytime = athlete_availability['time'] # e.g., 14:00:00
        timestamp = float(athlete_availability['timestamp'])

         # Need to compare using the time of the location in which the user will be tested!!!
        # time_zone = pytz.timezone(region + "/" + city)
        # local_time = datetime.now(time_zone)
        
        # Athlete is trying to schedule during the same day!!!
        if date == time_obj.today().strftime('%d/%m/%Y'):
            print('cant give same day!')
            response = {}
            response["status"] = "Failed to update"
            response["reason"] = "Cannot create/change availability during the same day!"
            return response, False
        
        # I thought about it and it made sense. The athlete shouldn't put availabilty
        # 5 minutes later or an hour later. There has to be at least 12 hours between 'right now'
        # and doping testing time. We can play with it
        if timestamp - time_now < 12 * 60 * 60:
            print("cant < 5 hours")
            return_obj = {}
            return_obj["status"] = "Failed to update"
            return_obj["reason"] = "You need to give availability 12 hours in advance!"
            return return_obj, False


        # Athlete shouldn't be able to give availability for 10 days ahead!
        if timestamp - time_now > 10 * 24 * 60 * 60:
            print("cant give 10 days ahead")
            return_obj = {}
            return_obj["status"] = "Failed to update"
            return_obj["reason"] = "Cannot create/change availability of 10 days later!"
            return return_obj, False

    print("PASSED CONDITIONS")
    return 'success', True

def add_availabilities_in_db(region_code, availabilities_response, persisted_data):
    success = True
    email = persisted_data['athlete_email']
    for athlete_availability in persisted_data['availabilities']:
        athlete_availability['isScheduled'] = False
        athlete_availability['athlete_email'] = email 
        availabilities_response.append(athlete_availability)
        db_addition = db[region_code + "-athletes"].insert_one(athlete_availability).inserted_id
        success = success and db_addition
    return success

def check_availability_exists(athlete_email, athlete_availability, time_now):
    date = athlete_availability['date']
    timestamp_new = float(athlete_availability['timestamp'])
    
    # Has he scheduled in EU collection before?
    eu_availability = list(db["EU-athletes"].find({"$and": [{"date": {"$eq": date}}, 
                                                            {"athlete_email": {"$eq": athlete_email}}]})) 
    if eu_availability:
        # NOT ALLOWED TO CHANGE
        return timestamp_new - time_now < 48 * 60 * 60


    # Has he scheduled in NA collection before?
    na_availability = list(db["NA-athletes"].find({"$and": [{"date": {"$eq": date}}, 
                                                            {"athlete_email": {"$eq": athlete_email}}]})) 
    if na_availability:
        # NOT ALLOWED TO CHANGE
        return timestamp_new - time_now < 48 * 60 * 60


    # Has he scheduled in AS collection before?
    as_availability = list(db["AS-athletes"].find({"$and": [{"date": {"$eq": date}}, 
                                                            {"athlete_email": {"$eq": athlete_email}}]})) 
    if as_availability:
        # NOT ALLOWED TO CHANGE
        return timestamp_new - time_now < 48 * 60 * 60


    # Has he scheduled in AU collection before?
    au_availability = list(db["AU-athletes"].find({"$and": [{"date": {"$eq": date}}, 
                                                            {"athlete_email": {"$eq": athlete_email}}]})) 
    if au_availability:
        # NOT ALLOWED TO CHANGE
        return timestamp_new - time_now < 48 * 60 * 60



# FUN PART :)
@app.route('/scheduleTesting/<continent_code>', methods=['GET'])
def schedule_testing(continent_code):
    # NA, EU, AS, AU ==> for sharding purposes only pass one for each 'schedule testing'
    # I hardcoded the region code on purpose.
    response_assignments = []
    
    # tomorrow's date, for which the assignment is made
    tomorrow_date = (datetime.datetime.today() + datetime.timedelta(days=1)).strftime('%d/%m/%Y')
    
    for country in continent_to_countries[continent_code]:
        athlete_availabilities = list(db[continent_code + "-athletes"].find({"$and": [{"isScheduled": {"$eq": False}}, 
                                                                                {"date": {"$eq": tomorrow_date}},
                                                                                {"country": {"$eq": country}}]})) 
        all_testers = list(db[continent_code + "-testers"].find({"country": {"$eq": country}}))
        print("ALL TESTERS ??====> " + str(all_testers))
        
        scheduled_testings = list(db[continent_code + "-assignments"].find({"$and": [{"date": {"$eq": tomorrow_date}},
                                                                                {"country": {"$eq": country}}]})) 
        
        athlete_to_times = defaultdict()
        booked_times_to_testers = defaultdict(set)
        athletes_emails = set()
        testers_emails = set()
        availability_to_athlete = {}
        
        # Tester Emails added to the tester list
        for agnt in all_testers:
            testers_emails.add(agnt['tester_email'])
        
        #print("TESTERS ==> " + str(testers_emails))
        # Keep track of every tester's already booked dates
        # This is to prevent double booking the same tester to different athletes at the same time
        for appointment in scheduled_testings:
            date, hour = appointment['date'], appointment['time']
            tester_email = appointment['tester_email']
            uniq_time_identifier = date + "_" + hour
            booked_times_to_testers[uniq_time_identifier].add(tester_email)
        
        for athlete_doc in athlete_availabilities:
            print("ATTTHLEEETE   === > " + str(athlete_doc))
            athlete_email = athlete_doc["athlete_email"]
            date, hour = athlete_doc['date'], athlete_doc['time']
            uniq_time_identifier = date + "_" + hour
            athlete_to_times[athlete_email] = uniq_time_identifier
            availability_to_athlete[athlete_email + "_" + uniq_time_identifier] = athlete_doc
            athletes_emails.add(athlete_email)
        
        for athlete in athletes_emails:
            requested_time = athlete_to_times[athlete]
            testing_possibility = random.randint(0, 9)
            if testing_possibility > 2:
                unavailable_testers = booked_times_to_testers[requested_time]
                available_testers = testers_emails - unavailable_testers
                print("AVAILABLE TESTERS: " + str(available_testers))
                if available_testers:
                    assigned_tester = available_testers.pop()
                    booked_times_to_testers[requested_time].add(assigned_tester) # Flag it as scheduled tester at the particular time!
                    # the key here is ==> sedat@gmail.com_24/04/2022_17:30:00 --> there can only be one such key! Thus, no ambiguity!
                    athlete_doc = availability_to_athlete[athlete + "_" + requested_time]
                    
                    # The assignment is made between the athlete and the tester for that particular date/time 
                    assignment_entry = {"athlete_email" : athlete, "tester_email" : assigned_tester, 
                                        "country": athlete_doc["country"], "region" : athlete_doc["region"],
                                        "date" : athlete_doc["date"], "time" : athlete_doc["time"], 
                                        "timestamp" : athlete_doc["timestamp"], "location" : athlete_doc["location"]}
                    
                    response_assignments.append(assignment_entry)
                    
                    # Add the scheduled testing
                    db[continent_code + "-assignments"].insert_one(assignment_entry)
                    
                    # Athlete on this date is scheduled so update "isScheduled" field to --> True 
                    db[continent_code + "-athletes"].update_one({'athlete_email': athlete, 'date':athlete_doc["date"]},
                                                                {"$set": { 'isScheduled': True }} )
                                
    if response_assignments:
        response = json.loads(dumps(response_assignments, indent=10))
    else: response = {"scheduled_athletes" : "NONE MADE"}
    return make_response(jsonify(response), 200)   
    
    
    
# SCHEDULE BY COUNTRY 
@app.route('/scheduleTestingCountry/<country>', methods=['GET'])
def schedule_testing_country(country):
    continent_code = countries_to_continent[country]
    response_assignments = []
    
    # tomorrow's date, for which the assignment is made
    tomorrow_date = (datetime.datetime.today() + datetime.timedelta(days=1)).strftime('%d/%m/%Y')
    
    athlete_availabilities = list(db[continent_code + "-athletes"].find({"$and": [{"isScheduled": {"$eq": False}}, 
                                                                            {"date": {"$eq": tomorrow_date}},
                                                                            {"country": {"$eq": country}}]})) 
    all_testers = list(db[continent_code + "-testers"].find({"country": {"$eq": country}}))
    print("ALL TESTERS ??====> " + str(all_testers))
    
    scheduled_testings = list(db[continent_code + "-assignments"].find({"$and": [{"date": {"$eq": tomorrow_date}},
                                                                            {"country": {"$eq": country}}]})) 
    
    athlete_to_times = defaultdict()
    booked_times_to_testers = defaultdict(set)
    athletes_emails = set()
    testers_emails = set()
    availability_to_athlete = {}
    
    # Tester Emails added to the tester list
    for agnt in all_testers:
        testers_emails.add(agnt['tester_email'])
    
    #print("TESTERS ==> " + str(testers_emails))
    # Keep track of every tester's already booked dates
    # This is to prevent double booking the same tester to different athletes at the same time
    for appointment in scheduled_testings:
        date, hour = appointment['date'], appointment['time']
        tester_email = appointment['tester_email']
        uniq_time_identifier = date + "_" + hour
        booked_times_to_testers[uniq_time_identifier].add(tester_email)
    
    for athlete_doc in athlete_availabilities:
        print("ATTTHLEEETE   === > " + str(athlete_doc))
        athlete_email = athlete_doc["athlete_email"]
        date, hour = athlete_doc['date'], athlete_doc['time']
        uniq_time_identifier = date + "_" + hour
        athlete_to_times[athlete_email] = uniq_time_identifier
        availability_to_athlete[athlete_email + "_" + uniq_time_identifier] = athlete_doc
        athletes_emails.add(athlete_email)
    
    for athlete in athletes_emails:
        requested_time = athlete_to_times[athlete]
        testing_possibility = random.randint(0, 9)
        if testing_possibility > 2:
            unavailable_testers = booked_times_to_testers[requested_time]
            available_testers = testers_emails - unavailable_testers
            print("AVAILABLE TESTERS: " + str(available_testers))
            if available_testers:
                assigned_tester = available_testers.pop()
                booked_times_to_testers[requested_time].add(assigned_tester) # Flag it as scheduled tester at the particular time!
                # the key here is ==> sedat@gmail.com_24/04/2022_17:30:00 --> there can only be one such key! Thus, no ambiguity!
                athlete_doc = availability_to_athlete[athlete + "_" + requested_time]
                
                # The assignment is made between the athlete and the tester for that particular date/time 
                assignment_entry = {"athlete_email" : athlete, "tester_email" : assigned_tester, 
                                    "country": athlete_doc["country"], "region" : athlete_doc["region"],
                                    "date" : athlete_doc["date"], "time" : athlete_doc["time"], 
                                    "timestamp" : athlete_doc["timestamp"], "location" : athlete_doc["location"]}
                
                response_assignments.append(assignment_entry)
                
                # Add the scheduled testing
                db[continent_code + "-assignments"].insert_one(assignment_entry)
                
                # Athlete on this date is scheduled so update "isScheduled" field to --> True 
                db[continent_code + "-athletes"].update_one({'athlete_email': athlete, 'date':athlete_doc["date"]},
                                                            {"$set": { 'isScheduled': True }} )
                              
    if response_assignments:
        response = json.loads(dumps(response_assignments, indent=10))
    else: response = {"scheduled_athletes" : "NONE MADE"}
    return make_response(jsonify(response), 200)                   




# get the specific tester's entire schedule
@app.route('/getTesterSchedule/<tester_email>', methods=['GET'])
def get_tester_schedule(tester_email):
    # NA, EU, AS, AU ==> for sharding purposes only pass one for each 'schedule testing'
    # I hardcoded the region code on purpose.
    tester_appointments = []
    for continent_code in continent_to_countries:
        
        curr_timestamp = time.time()
        tester_schedule = list(db[continent_code + "-assignments"].find({"$and": [{"tester_email": {"$eq": tester_email}},
                                                                                {"timestamp": {"$gt": curr_timestamp}}]}))
        if tester_schedule:
            tester_appointments.append({continent_code : tester_schedule})

    return make_response(jsonify(json.loads(dumps(tester_appointments, indent=10))), 200)
   

# If athletes wants to see what future availabilities he gave and wants to remember them, so 
# he can refer to them to change them if he wants to 
@app.route('/getAthleteAvailabilities/<athlete_email>', methods=['GET'])
def get_athlete_availabilities(athlete_email):
    athlete_availabilities = []
    curr_timestamp = time.time()
    for continent_code in continent_to_countries:
        athlete_schedule = list(db[continent_code + "-athletes"].find({"$and": [{"athlete_email": {"$eq": athlete_email}},
                                                                                {"timestamp": {"$gt": curr_timestamp}}]}))
        if athlete_schedule:
            # Gotta remove the 'isScheduled' field from the dict bc athlete shouldn't see if scheduled xD
            for availability_dict in athlete_schedule:
                del availability_dict['isScheduled']
            athlete_availabilities.append({continent_code : athlete_schedule})

    return make_response(jsonify(json.loads(dumps(athlete_availabilities, indent=10))), 200)



# For administrators to see today's schedule by region (tester - athlete assignments).
@app.route('/getTodaySchedule', methods=['GET'])
def get_today_schedule():
    today_date = datetime.datetime.today().strftime('%d/%m/%Y')
    print(today_date)
    today_schedule = []
    for continent_code in continent_to_countries:
        assignments_per_region = list(db[continent_code + "-assignments"].find({"date": {"$eq": today_date}}))
        
        if assignments_per_region:
            today_schedule.append(assignments_per_region)

    return make_response(jsonify(json.loads(dumps(today_schedule, indent=10))), 200)




if __name__ == '__main__':
    app.run(host="127.0.0.1", port = 8080, debug=True)
