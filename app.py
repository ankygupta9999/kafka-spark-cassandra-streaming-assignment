# Importing libraries
from time import process_time
from faker import Faker
import pandas as pd
import yaml
import os
import json
import random
import datetime
from producer import kafka_producer as producer

interests = ["cricket","badminton","swimming","chess","volleyball","technology","mobiles","nasa","space","kickboxing"]
communities = ["sports","science","tech"]

def gen_rand_user(fake):
    user_id="U"+ str(random.randrange(1000000, 2000000, 1))
    user_nm=fake.name()
    user_city=fake.city()
    user_st=fake.state()
    user_interests=random.choice(interests)
    user_comms=random.choice(communities)
    # user_comms=random.choices(communities, k=random.randint(1,3))
    user_comm_cnt=len(user_comms)
    user_created=str(datetime.datetime.now())
    
    user_dict = {"user_id":user_id,
            "user_nm":user_nm,
            "user_city":user_city,
            "user_st":user_st,
            "user_interests":user_interests,
            "user_comms":user_comms,
            "user_comm_cnt":user_comm_cnt,
            "user_created":user_created}
    return user_dict

def gen_fake_posts(fake: Faker, user): 
  
    post_data = {}
    post_data["post_id"] = "P"+str(random.randrange(10000000, 20000000, 1))
    post_data["post_type"] = random.choice(["Community","User","Comment"])
    post_data["post_action"] = random.choice(["Like","Dislike","New"])
    post_data["post_in_comm"] = "COM"+str(random.randrange(10000, 30000, 3))
    post_data["post_msg"] = fake.paragraph()
    post_data["post_by"] = user["user_id"]
    # post_data["post_likes"]
    post_data["post_tstamp"]=str(datetime.datetime.now())

    print(post_data) 
    return post_data
  
    # dictionary dumped as json in a json file 
    # with open('students.json', 'w') as fp: 
    #     json.dump(student_data, fp) 

if __name__ == "__main__":
    print ("Starting main app flow")

    #Get config
    with open("config.yaml", "r") as ymlfile:
        cfg = yaml.safe_load(ymlfile)
    
    print (cfg["kafka"])

    # Instantiate faker
    fake = Faker('en_IN')
    

    kproducer = producer.get_kafka_producer(cfg["kafka"]["broker"])
    
    #produce sample msgs:
    post_data ={} 
    newuser =[]
    newpost = []
    for i in range(0, 10):
        newuser.append(gen_rand_user(fake))
        producer._produce_msg(kproducer, cfg["kafka"]["tgt_topic"],str(newuser[i]["user_id"]),str(newuser[i]))

        # for j in range(0,15):
        #     #generate post by above user
        #     newpost.append(gen_fake_posts(fake, newuser[i]))
        #     producer._produce_msg(kproducer, cfg["kafka"]["tgt_topic"],str(newpost[j]["post_id"]),str(newpost[j]))
