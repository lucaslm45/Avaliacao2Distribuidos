from confluent_kafka import Producer
from faker import Faker
import json
import time
import logging
import sys
import random

fake=Faker()

p=Producer({'bootstrap.servers':'localhost:9092'})

print('Kafka Producer has been initiated...')

def main():
    for i in range(100):
        data={
           'user_id': fake.random_int(min=20000, max=100000),
           'user_name':fake.name(),
           'user_address':fake.street_address() + ' | ' + fake.city() + ' | ' + fake.country_code(),
           'platform': random.choice(['Mobile', 'Laptop', 'Tablet']),
           'signup_at': str(fake.date_time_this_month()),
           'message': str(fake.text())   

           }
        m=json.dumps(data)
        p.poll(1)
        p.produce('user-tracker',m.encode('utf-8'))#,callback=receipt)
        p.flush()
        time.sleep(3)
        
if __name__ == '__main__':
    main()