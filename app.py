import json
import os
import requests
import random
import datetime
from textwrap import indent
import time
from urllib import response
from flask import Flask, jsonify, request, make_response
from bson.json_util import dumps
from flask_cors import CORS
from pymongo import MongoClient
from collections import defaultdict
from flask import Response

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
    location = os.environ["APP_LOCATION"]
    return make_response(location, 200)


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
@app.route('/createAvailability', methods=['POST'])
def create_availability():
    availabilities_response = []
    availability_data = request.get_json()
    athlete_email = availability_data['athlete_email']
    availabilities = availability_data['availabilities']

    time_now = time_obj.now().timestamp()
    regions_availabilities_dict = {'North America':{'athlete_email':athlete_email, 'availabilities':[]},
                                     'Europe':{'athlete_email':athlete_email, 'availabilities':[]}, 
                                    'Australia':{'athlete_email':athlete_email, 'availabilities':[]},
                                     'Asia':{'athlete_email':athlete_email, 'availabilities':[]}}

    for a in availabilities:
        regions_availabilities_dict[a['region']]['availabilities'].append(a)

    url = "https://internalLoadBalancerFrontDoor.azurefd.net/createAvailability"
    location = os.environ["APP_LOCATION"]
    for key in regions_availabilities_dict.keys():
        if key != location:
            if(len(regions_availabilities_dict[key]['availabilities']) > 0):
                headers = {"x-preferred-backend": key} 
                data = regions_availabilities_dict[key]
                response = requests.post(url, headers=headers, json=data)
                if(response.status_code != 200):
                    availabilities_response.append("Not nice: " + key)
        else:
            for athlete_availability in regions_availabilities_dict[key]['availabilities']:
                region = athlete_availability['region']
                country = athlete_availability['country']
                #city = athlete_availability['city']
                date = athlete_availability['date']  # Day/Month/Year
                daytime = athlete_availability['time'] # e.g., 14:00:00
                timestamp = float(athlete_availability['timestamp'])
                # print('timestamp = ' + str(timestamp))
                # print('difference = ' + str(timestamp - time_obj.now().timestamp()))
                
                # Need to compare using the time of the location in which the user will be tested!!!
                # time_zone = pytz.timezone(region + "/" + city)
                # local_time = datetime.now(time_zone)
                
                # Athlete is trying to schedule during the same day!!!
                if date == time_obj.today().strftime('%d/%m/%Y'):
                    print('cant give same day!')
                    response = {}
                    response["status"] = "Failed to update"
                    response["reason"] = "Cannot create/change availability during the same day!"
                    return make_response(jsonify(response), 406)
                
                # I thought about it and it made sense. The athlete shouldn't put availabilty
                # 5 minutes later or an hour later. There has to be at least 12 hours between 'right now'
                # and doping testing time. We can play with it
                if timestamp - time_now < 12 * 60 * 60:
                    print("cant < 5 hours")
                    return_obj = {}
                    return_obj["status"] = "Failed to update"
                    return_obj["reason"] = "You need to give availability 12 hours in advance!"
                    return make_response(jsonify(return_obj), 406)


                # Athlete shouldn't be able to give availability for 10 days ahead!
                if timestamp - time_now > 10 * 24 * 60 * 60:
                    print("cant give 10 days ahead")
                    return_obj = {}
                    return_obj["status"] = "Failed to update"
                    return_obj["reason"] = "Cannot create/change availability of 10 days later!"
                    return make_response(jsonify(return_obj), 406)

                # If user already had an appointment for that particular day, remove it
                # because we're going to update it with the new availability info.
                # Since the prev availability might be in any of the regions, we should try to delete each 
                
                db["NA-athletes"].delete_one({ "$and": [{"athlete_email": {"$eq": athlete_email}}, 
                                {"date": {"$eq": date}} ] })
                db["EU-athletes"].delete_one({ "$and": [{"athlete_email": {"$eq": athlete_email}}, 
                                {"date": {"$eq": date}} ] })
                db["AS-athletes"].delete_one({ "$and": [{"athlete_email": {"$eq": athlete_email}}, 
                                {"date": {"$eq": date}} ] })
                db["AU-athletes"].delete_one({ "$and": [{"athlete_email": {"$eq": athlete_email}}, 
                                {"date": {"$eq": date}} ] })        

                # If the appointment has already been scheduled, then remove the appointment 
                # of the athlete. The appointment is always a one-to-one match with
                # athlete_email and date
                
                db["NA-assignments"].delete_one({ "$and": [{"athlete_email": {"$eq": athlete_email}}, 
                                {"date": {"$eq": date}} ] })
                db["EU-assignments"].delete_one({ "$and": [{"athlete_email": {"$eq": athlete_email}}, 
                                {"date": {"$eq": date}} ] })
                db["AS-assignments"].delete_one({ "$and": [{"athlete_email": {"$eq": athlete_email}}, 
                                {"date": {"$eq": date}} ] })
                db["AU-assignments"].delete_one({ "$and": [{"athlete_email": {"$eq": athlete_email}}, 
                                {"date": {"$eq": date}} ] })
                    
                region_code = region_to_code[location]
                
                # This is a sample entry added to our db (1 availability)
                # for each availability, we have an object of the same type.
        #           {
        #               athlete_email: 'sedat@gmail.com'               
        #               region: 'Europe',
        #               country: 'Ireland',
        #               location: 'SLS'
        #               city: 'Dublin',
        #               date: 24/04/2022
        #               time: 16:00:00,
        #               timestamp: 3434334343,
        #      @@@@@@@  isScheduled: FALSE @@@@@@@@@@@ (we add this below)
        #          },
                
                # Before adding the availability, make sure to flag the availabbility as 'unscheduled'!
                athlete_availability['isScheduled'] = False
                athlete_availability['athlete_email'] = athlete_email
                print("PASSED CONDITIONS")
                #print(athlete_availability)
                availabilities_response.append(athlete_availability)
                # Sharding based on region_code (continent)
                db_addition = db[region_code + "-athletes"].insert_one(athlete_availability).inserted_id
        
    return make_response(jsonify(json.loads(dumps(availabilities_response, indent=10))), 200)
    
        # db["NA-athletes"].replace_one(
        #     { "athlete_email": athlete_email, "date" : date }, athlete_availability
        # )



# FUN PART :)
@app.route('/scheduleTesting/<continent_code>', methods=['GET'])
def schedule_testing(continent_code):
    # NA, EU, AS, AU ==> for sharding purposes only pass one for each 'schedule testing'
    # I hardcoded the region code on purpose.
    response_assignments = []
    curr_timestamp = time.time()
    for country in continent_to_countries[continent_code]:
        athlete_availabilities = list(db[continent_code + "-athletes"].find({"$and": [{"isScheduled": {"$eq": False}}, 
                                                                             {"timestamp": {"$gt": curr_timestamp}},
                                                                             {"country": {"$eq": country}}]})) 
        all_testers = list(db[continent_code + "-testers"].find({"country": {"$eq": country}}))
        print("ALL TESTERS ??====> " + str(all_testers))
        
        scheduled_testings = list(db[continent_code + "-assignments"].find({"$and": [{"timestamp": {"$gt": curr_timestamp}},
                                                                             {"country": {"$eq": country}}]})) 
        
        athlete_to_times = defaultdict(set)
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
            athlete_email = athlete_doc["athlete_email"]
            date, hour = athlete_doc['date'], athlete_doc['time']
            uniq_time_identifier = date + "_" + hour
            athlete_to_times[athlete_email].add(uniq_time_identifier)
            availability_to_athlete[athlete_email + "_" + uniq_time_identifier] = athlete_doc
            athletes_emails.add(athlete_email)
        
        for athlete in athletes_emails:
            for requested_time in athlete_to_times[athlete]:
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
    curr_timestamp = time.time()
    
    athlete_availabilities = list(db[continent_code + "-athletes"].find({"$and": [{"isScheduled": {"$eq": False}}, 
                                                                            {"timestamp": {"$gt": curr_timestamp}},
                                                                            {"country": {"$eq": country}}]})) 
    all_testers = list(db[continent_code + "-testers"].find({"country": {"$eq": country}}))
    print("ALL TESTERS ??====> " + str(all_testers))
    
    scheduled_testings = list(db[continent_code + "-assignments"].find({"$and": [{"timestamp": {"$gt": curr_timestamp}},
                                                                            {"country": {"$eq": country}}]})) 
    
    athlete_to_times = defaultdict(set)
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
        athlete_email = athlete_doc["athlete_email"]
        date, hour = athlete_doc['date'], athlete_doc['time']
        uniq_time_identifier = date + "_" + hour
        athlete_to_times[athlete_email].add(uniq_time_identifier)
        availability_to_athlete[athlete_email + "_" + uniq_time_identifier] = athlete_doc
        athletes_emails.add(athlete_email)
    
    for athlete in athletes_emails:
        for requested_time in athlete_to_times[athlete]:
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
   


if __name__ == '__main__':
    app.run(host="127.0.0.1", port = 8080, debug=True)
