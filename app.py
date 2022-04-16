import json
import os
import random
import datetime
import time
from urllib import response
from flask import Flask, jsonify, request, make_response
from bson.json_util import dumps
from flask_cors import CORS
from pymongo import MongoClient
from collections import defaultdict

from db_commands import CONNECTION_STRING # CONOR's string

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

continent_to_countries = {'Europe' : ['France', 'Ireland', 'England'],
                          'North America' : ['Canada', 'America'],
                          'Asia' : ['China', 'India', 'Japan'],
                          'Australia' : ['Australia']}



@app.route('/')
def root():
    return make_response("ouba eeee", 200)


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
    availability_data = request.get_json()
    athlete_email = availability_data['athlete_email']
    availabilities = availability_data['availabilities']

    for athlete_availability in availabilities:
        athlete_availability = athlete_availability.to_dict()
        region = athlete_availability['region']
        country = athlete_availability['country']
        location = athlete_availability['location']
        #city = athlete_availability['city']
        date = athlete_availability['date']  # Day/Month/Year
        time = athlete_availability['time'] # e.g., 14:00:00
        timestamp = float(athlete_availability['timestamp'])
        
        # Need to compare using the time of the location in which the user will be tested!!!
        # time_zone = pytz.timezone(region + "/" + city)
        # local_time = datetime.now(time_zone)
        
        # Athlete is trying to schedule during the same day!!!
        if date == time_obj.today().strftime('%d/%m/%Y'):
            response = {}
            response["status"] = "Failed to update"
            response["reason"] = "Cannot create/change availability during the same day!"
            return make_response(jsonify(response), 200)
        
        # I thought about it and it made sense. The athlete shouldn't put availabilty
        # 5 minutes later or an hour later. There has to be at least 12 hours between 'right now'
        # and doping testing time. We can play with it
        if timestamp - time_obj.time() < 60 * 60 * 12:
            return_obj = {}
            return_obj["status"] = "Failed to update"
            return_obj["reason"] = "You need to give availability 12 hours in advance!"
            return make_response(jsonify(return_obj), 200)


        # Athlete shouldn't be able to give availability for 10 days ahead!
        if timestamp - time_obj.time() >= 60 * 60 * 10:
            return_obj = {}
            return_obj["status"] = "Failed to update"
            return_obj["reason"] = "Cannot create/change availability of 10 days later!"
            return make_response(jsonify(return_obj), 200)

        # If user already had an appointment for that particular day, remove it
        # because we're going to update it with the new availability info.
        # Since the prev availability might be in any of the regions, we should try to delete each 
        db["NA-athletes"].delete({ "$and": [{"athlete_email": {"$eq": athlete_email}}, 
                        {"date": {"$eq": date}} ] })
        db["EU-athletes"].delete({ "$and": [{"athlete_email": {"$eq": athlete_email}}, 
                        {"date": {"$eq": date}} ] })
        db["AS-athletes"].delete({ "$and": [{"athlete_email": {"$eq": athlete_email}}, 
                        {"date": {"$eq": date}} ] })
        db["AU-athletes"].delete({ "$and": [{"athlete_email": {"$eq": athlete_email}}, 
                        {"date": {"$eq": date}} ] })        

        # If the appointment has already been scheduled, then remove the appointment 
        # of the athlete. The appointment is always a one-to-one match with
        # athlete_email and date
        db["NA-assignments"].delete({ "$and": [{"athlete_email": {"$eq": athlete_email}}, 
                        {"date": {"$eq": date}} ] })
        db["EU-assignments"].delete({ "$and": [{"athlete_email": {"$eq": athlete_email}}, 
                        {"date": {"$eq": date}} ] })
        db["AS-assignments"].delete({ "$and": [{"athlete_email": {"$eq": athlete_email}}, 
                        {"date": {"$eq": date}} ] })
        db["AU-assignments"].delete({ "$and": [{"athlete_email": {"$eq": athlete_email}}, 
                        {"date": {"$eq": date}} ] })
            
        region_code = region_to_code[region]
        
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
        # Sharding based on region_code (continent)
        db_addition = db[region_code + "-athletes"].insert_one({"random_id" : {"Conor":"Church"}}).inserted_id
        
        return make_response(jsonify("athlete_availability"), 200)
    
        # db["NA-athletes"].replace_one(
        #     { "athlete_email": athlete_email, "date" : date }, athlete_availability
        # )



# FUN PART :)
@app.route('/scheduleTesting', methods=['POST'])
def schedule_testing():
    # NA, EU, AS, AU ==> for sharding purposes only pass one for each 'schedule testing'
    # I hardcoded the region code on purpose.
    continent_code = 'EU'
    curr_timestamp = time.time()
    for country in continent_to_countries[continent_code]:
        athlete_availabilities = db[continent_code + "-athletes"].find({"$and": [{"isScheduled": {"$eq": False}}, 
                                                                             {"timestamp": {"$gt": curr_timestamp}},
                                                                             {"country": {"$eq": country}}]}) 
        all_testers = db[continent_code + "-testers"].find({"country": {"$eq": country}}) 
        
        scheduled_testings = db[continent_code + "-assignments"].find({"$and": [{"timestamp": {"$gt": curr_timestamp}},
                                                                             {"country": {"$eq": country}}]}) 
        
        athlete_to_times = defaultdict(set)
        booked_times_to_testers = defaultdict(set)
        athletes_emails = set()
        testers_emails = set()
        availability_to_athlete = {}
        
        # Tester Emails added to the tester list
        for agnt in all_testers:
            testers_emails.add(agnt['tester_email'])
        
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
                if testing_possibility > 7:
                    unavailable_testers = booked_times_to_testers[requested_time]
                    available_testers = all_testers - unavailable_testers
                    if available_testers:
                        assigned_tester = available_testers[0]
                        booked_times_to_testers[requested_time].add(assigned_tester) # Flag it as scheduled tester at the particular time!
                        # the key here is ==> sedat@gmail.com_24/04/2022_17:30:00 --> there can only be one such key! Thus, no ambiguity!
                        athlete_doc = availability_to_athlete[athlete + "_" + requested_time]
                        
                        # The assignment is made between the athlete and the tester for that particular date/time 
                        assignment_entry = {"athlete_email" : athlete, "tester_email" : assigned_tester, 
                                            "country": athlete_doc["country"], "region" : athlete_doc["region"],
                                            "date" : athlete_doc["date"], "time" : athlete_doc["time"], 
                                            "timestamp" : athlete_doc["timestamp"], "location" : athlete_doc["location"]}
                        
                        # Add the scheduled testing
                        db[continent_code + "-assignments"].insert_one(assignment_entry)
                        
                        # Athlete on this date is scheduled so update "isScheduled" field to --> True 
                        db[continent_code + "-athletes"].update_one({'athlete_email': athlete, 'date':athlete_doc["date"]},
                                                                   {"$set": { 'isScheduled': True }} )
                              
        response = {"status" : "Success"}
        return make_response(jsonify(response), 200)                    
        

@app.route('/getTesterSchedule/<tester_email>', methods=['GET'])
def get_tester_schedule(tester_email):
    # NA, EU, AS, AU ==> for sharding purposes only pass one for each 'schedule testing'
    # I hardcoded the region code on purpose.
    continent_code = 'EU'
    curr_timestamp = time.time()
    tester_schedule = db[continent_code + "-assignments"].find({"$and": [{"tester_email": {"$eq": tester_email}},
                                                                             {"timestamp": {"$gt": curr_timestamp}}]}) 
    response_json = dumps(tester_schedule)
    return make_response(jsonify(response_json), 200) 
    

if __name__ == '__main__':
    app.run(host="127.0.0.1", port = 8080, debug=True)