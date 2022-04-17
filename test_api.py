"""
-*- coding: utf-8 -*-
@File  : test_api.py.py
@Author: Yuki
@Date  : 17/04/2022
@Software : PyCharm
"""
import datetime

import requests

url_schedule = "http://127.0.0.1:5000/scheduleTesting"
r1 = requests.post(url_schedule)
print(r1.json())

# url_submit = "http://127.0.0.1:5000/createAvailability"
# timestamp = datetime.datetime(2022, 4, 19, 9).timestamp()
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
# r2 = requests.post(url_submit, json=data)
# print(r2.json())
