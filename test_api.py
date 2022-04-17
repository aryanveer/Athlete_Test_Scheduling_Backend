"""
-*- coding: utf-8 -*-
@File  : test_api.py.py
@Author: Yuki
@Date  : 17/04/2022
@Software : PyCharm
"""
from datetime import datetime, timedelta
import requests

# url_submit = "http://127.0.0.1:5000/createAvailability"
# delta = timedelta(days=3)
# n_days_after = datetime.now() + delta
# timestamp = n_days_after.timestamp()
# data = {
#     'athlete_email': 'test@test.com',
#     'availabilities': [{
#         'region': 'Europe',
#         'location': 'SLS',
#         'country': 'Ireland',
#         'date': '19/04/2022',
#         'time': '09:00',
#         'timestamp': 1650392369
#     }]
# }
# r1 = requests.post(url_submit, json=data)
# print(r1.json())
#
# url_schedule = "http://127.0.0.1:5000/scheduleTesting"
# r2 = requests.post(url_schedule)
# print(r2.json())

# url_getTest = "http://127.0.0.1:5000/getTesterSchedule/mark16@gmail.com"
# r3 = requests.get(url_getTest)
# print(r3.json())


url_submitTest = "http://127.0.0.1:5000/submitTesterSchedule/625c86c4c630d5db0b672189"
r3 = requests.get(url_submitTest)
print(r3)
