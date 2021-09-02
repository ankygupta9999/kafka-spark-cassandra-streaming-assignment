#from vega_datasets import data
import streamlit as st
import altair as alt
import pandas as pd
import matplotlib.pyplot as plt
from cassandra.cluster import Cluster

def main():
    dfUsers = load_data("users")
    dfCommunity = load_data("community")
    dfPost = load_data("post")
    page = st.sidebar.selectbox("Choose a page", ["Homepage", "Reports", "BarChart"])

    if page == "Homepage":
        st.header("This is SPA Assignment for Group 41.")
        st.write("Please select a page on the left.")
        st.write(dfUsers)
        st.write(dfCommunity)
        st.write(dfPost)
    elif page == "Reports":
        st.title("SPA Assignment Stats")
        visualize_data_circle(dfUsers, 'user_interests', ['id', 'user_city', 'user_comm_cnt', 'user_comms', 'user_created', 'user_id', 'user_interests', 'user_nm', 'user_st'])
        visualize_data_circle(dfCommunity, 'comm_nm', ['comm_id', 'comm_nm', 'comm_posts_cnt', 'comm_posts_id','comm_tstamp', 'comm_users','comm_users_cnt'])
        visualize_data_circle(dfPost, 'post_dislikes', ['post_action','post_by', 'post_comments', 'post_dislikes','post_id','post_in_comm','post_likes', 'post_msg', 'post_tstamp', 'post_type'])
    elif page == "BarChart":
        st.title("SPA Assignment Stats")
        visualize_data_bar(dfPost, 'post_dislikes', ['post_action','post_by', 'post_comments', 'post_dislikes','post_id','post_in_comm','post_likes', 'post_msg', 'post_tstamp', 'post_type'])
        visualize_data_bar(dfCommunity, 'comm_nm', ['comm_id', 'comm_nm', 'comm_posts_cnt', 'comm_posts_id','comm_tstamp', 'comm_users','comm_users_cnt'])
        visualize_data_bar(dfUsers,'user_interests', ['id', 'user_city', 'user_comm_cnt', 'user_comms', 'user_created', 'user_id', 'user_interests', 'user_nm', 'user_st'])

@st.cache
def load_data(datatype):
    #df = data.cars()
    cluster = Cluster(['127.0.0.1'],port=9042)
    session = cluster.connect('raw_stream',wait_for_all_pools=True)
    session.execute('USE raw_stream')
    if datatype == "post":
        sql_query = 'SELECT * FROM post'
    elif datatype == "community":
        sql_query = 'SELECT * FROM community'
    elif datatype == "users":
        sql_query = 'SELECT * FROM users'
    df = pd.DataFrame()
    df = pd.DataFrame(list(session.execute(sql_query)))
    return df

def visualize_data_circle(df, icolor, itooltip):
    x_axis = st.selectbox("Choose a variable for the x-axis", df.columns, index=3)
    y_axis = st.selectbox("Choose a variable for the y-axis", df.columns, index=4)
    graph = alt.Chart(df).mark_circle(size=60).encode(
        x=x_axis,
        y=y_axis,
        color=icolor,
        tooltip=itooltip

    ).interactive()

    st.write(graph)

def visualize_data_bar(df, icolor, itooltip):
    x_axis = st.selectbox("Choose a variable for the x-axis", df.columns, index=3)
    y_axis = st.selectbox("Choose a variable for the y-axis", df.columns, index=4)
    graph = alt.Chart(df).mark_bar(size=60).encode(
        x=x_axis,
        y=y_axis,
        color=icolor,
        tooltip=itooltip
    ).interactive()
    st.write(graph)



if __name__ == "__main__":
    main()