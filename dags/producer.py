import time
import logging
import traceback

from faker import Faker
import base64
import json
import os
import datetime

fake = Faker()


def create_fake_person(iterations: int):
    for _ in range(iterations):
        data = {
            "name": fake.name(),
            "company": fake.company(),
            "msg": fake.sentence(),
            "remote_ip": fake.ipv4_public(),
            "user_agent": fake.user_agent(),
            "date": str(fake.date_between("today", "+8h"))
        }

        data_strings = json.dumps(data)
        data_bytes = data_strings.encode("ascii")
        base64_log = base64.b64encode(data_bytes)
        base64_log_string = base64_log.decode('utf-8')
        time.sleep(3)

        try:
            "./data/created_logs/fake_person-{datetime.date.today()"
            with open(f"./data_logs/created_logs/information_creation-{datetime.date.today()}.log", "a+") as log_file:
                log_file.write(base64_log_string)
                log_file.write("\n")
                #print("logging start")
            # print(base64_log)
            # print(type(base64_log))
        except Exception as e:
            logging.error(traceback.format_exc())
        else:
            print("logging completed")


if __name__ == "__main__":
    create_fake_person(5)