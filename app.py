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

def gen_rand_user(fake, comm_ids):
    user_id="U"+ str(random.randrange(1000000, 2000000, 1))
    user_nm=fake.name()
    user_city=fake.city()
    user_st=fake.state()
    user_interests=random.choice(interests)
    user_comms=random.choice(comm_ids)
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

def gen_fake_posts(fake: Faker, user, user_comms): 
  
    post_data = {}
    post_data["post_id"] = "P"+str(random.randrange(10000000, 20000000, 1))
    post_data["post_type"] = random.choice(["Community","User","Comment"])
    post_data["post_action"] = random.choice(["Like","Dislike","New"])
    post_data["post_in_comm"] = user_comms
    post_data["post_msg"] = fake.paragraph()
    post_data["post_by"] = user["user_id"]
    post_data["post_likes"]= 0
    post_data["post_dislikes"] = 0
    post_data["post_comments"] = 0
    post_data["post_tstamp"]=str(datetime.datetime.now())

    print(post_data) 
    return post_data
  
def gen_communities(community_id, community): 
  
    comm_data = {}
    id = 100000 + community_id
    comm_data["comm_id"] = "COM"+str(id)
    comm_data["comm_nm"] = community
    comm_data["comm_users_cnt"]= 0
    comm_data["comm_users"] = []
    comm_data["comm_posts_cnt"] = 0
    comm_data["comm_posts_id"] = []
    comm_data["comm_tstamp"]=str(datetime.datetime.now())

    print(comm_data) 
    return comm_data

if __name__ == "__main__":
    print ("Starting main app flow")

    #Get config
    with open("config.yaml", "r") as ymlfile:
        cfg = yaml.safe_load(ymlfile)
    
    print (cfg["kafka"])

    # Instantiate faker
    fake = Faker('en_IN')
    

    kproducer = producer.get_kafka_producer(cfg["kafka"]["broker"])
    

    # add communities
    communities = ["sports","science","tech"]
    comm_ids = []
    for i, community in enumerate(communities):
        comm_data = gen_communities(i, community)
        comm_ids.append(comm_data["comm_id"] )
        producer._produce_msg(kproducer, cfg["kafka"]["tgt_topic"],str(comm_data["comm_id"]),str(comm_data))

    #produce sample msgs:
    post_data ={} 
    newuser =[]
    newpost = []
    for i in range(0, 10):
        newuser.append(gen_rand_user(fake, comm_ids))
        producer._produce_msg(kproducer, cfg["kafka"]["tgt_topic"],str(newuser[i]["user_id"]),str(newuser[i]))

        for j in range(0,15):
            #generate post by above user
            newpost.append(gen_fake_posts(fake, newuser[i], newuser[i]['user_comms']))
            producer._produce_msg(kproducer, cfg["kafka"]["tgt_topic"],str(newpost[j]["post_id"]),str(newpost[j]))
