import datetime
import random
import time
from builtins import range

from faker import Faker


def web_service_generate():
    fake = Faker('en_US')
    fake1 = Faker('en_GB')  # To generate phone numbers

    headers = ["Email Id", "Prefix", "Name", "Birth Date", "Phone Number", "Additional Email Id",
               "Address", "Zip Code", "City", "State", "Country", "Year", "Time", "Link", "Text"]

    records = random.randint(0, 10)

    print("Requesting data from API")
    # database query time mockup so its not exact timings
    # time.sleep(random.randint(3,20))
    print("Data recieved")

    filename = 'people_data_' + time.strftime("%Y%m%d-%H%M%S") + '.csv'

    client_packet = []
    for i in range(records):
        full_name = fake.name()
        FLname = full_name.split(" ")
        Fname = FLname[0]
        Lname = FLname[1]
        domain_name = "@testDomain.com"
        userId = Fname + "." + Lname + domain_name

        client_packet.append({
            "Email Id": userId,
            "Prefix": fake.prefix(),
            "Name": fake.name(),
            "Birth Date": fake.date(pattern="%d-%m-%Y", end_datetime=datetime.date(2000, 1, 1)),
            "Phone Number": fake1.phone_number(),
            "Additional Email Id": fake.email(),
            "Address": fake.address(),
            "Zip Code": fake.zipcode(),
            "City": fake.city(),
            "State": fake.state(),
            "Country": fake.country(),
            "Year": fake.year(),
            "Time": fake.time(),
            "Link": fake.url(),
            "Text": fake.word(),
        })
    sales_packet = []

    for c in client_packet:
        items = random.randint(1, 10)
        for item in range(items):
            volume = random.randint(1, 20)
            price = float(random.randint(99, 39999) / 100)

            sales_packet.append({
                "Email Id": c['Email Id'],
                "Item Id": random.randint(100, 500),
                "Name": fake.catch_phrase(),
                "Volume": volume,
                "Price": price,
                "Sales": price * volume
            })

    print("Data written")
    return [{"table_name": "clients", "data": client_packet}, {"table_name": "sales", "data": sales_packet}]