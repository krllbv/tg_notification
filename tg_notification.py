#importing libraries
import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandahouse
import pandas as pd

from airflow import DAG
from airflow.operators.python_operator import PythonOperator 
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

#Connecting to ClickHouse
connection = {
        'host': '***',
        'password': '**',
        'user': '***',
        'database': '***'}
#default parametrs for airflow tasks    
default_args = {
    'owner': 'k-begunov-18',
    'depends_on_past': False, 
    'retries': 2, 
    'retry_delay': timedelta(minutes=5),  
    'start_date': datetime (2023, 5, 17)}
#Launch interval of a DAG
schedule_interval = '0 11 * * *'
#DAG
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def krllb_tg_report():
            chat_id = 715361296
            #Bot settings
            my_token = '***'
            bot = telegram.Bot(token=my_token)
            #text message
            msg = """Feed_actions Report for {date} 
            DAU: {users} ({users_day_ago:+.1%} day ago, {users_week_ago:+.1%} week ago)
            Likes: {likes} ({likes_day_ago:+.1%} day ago, {likes_week_ago:+.1%} week ago)
            Views: {views} ({views_day_ago:+.1%} day ago, {views_week_ago:+.1%} week ago)
            CTR: {ctr} ({ctr_day_ago:+.1%} day ago, {ctr_week_ago:+.1%} week ago)
            """
            #function for extracting data
            @task()
            def get_feeds():
                q = """
                        select 
                              toDate(time) as date, 
                              count(distinct user_id) as DAU, 
                              countIf(action = 'like') as likes, 
                              countIf(action = 'view') as views, 
                              countIf(action = 'like') / countIf(action = 'view') as CTR
                        from 
                        simulator_20230420.feed_actions 
                        where toDate(time) between today() - 8 and today() - 1 
                        group by toDate(time) 
                        order by 1
                """
                df = pandahouse.read_clickhouse(q, connection=connection)
                return df
            #loading data into the message
            @task()
            def load_report1(df):
                today = pd.Timestamp('now') - pd.DateOffset(days=1)
                day_ago = today - pd.DateOffset(days=1)
                week_ago = today - pd.DateOffset(days=7)

                df['date'] = pd.to_datetime(df['date']).dt.date
                df = df.astype({'DAU':int, 'views':int, 'likes':int})

                report = msg.format(date = today.date(),

                                    users =df[df['date'] == today.date()]['DAU'].iloc[0],
                                    users_day_ago = (df[df['date'] == today.date()]['DAU'].iloc[0]
                                                           - df[df['date'] == day_ago.date()]['DAU'].iloc[0])
                                                           /df[df['date'] == day_ago.date()]['DAU'].iloc[0],
                                    users_week_ago = (df[df['date'] == today.date()]['DAU'].iloc[0]
                                                           - df[df['date'] == week_ago.date()]['DAU'].iloc[0])
                                                           /df[df['date'] == week_ago.date()]['DAU'].iloc[0],  

                                    likes=df[df['date'] == today.date()]['likes'].iloc[0],
                                    likes_day_ago=(df[df['date'] == today.date()]['likes'].iloc[0]
                                                           - df[df['date'] == day_ago.date()]['likes'].iloc[0])
                                                           /df[df['date'] == day_ago.date()]['likes'].iloc[0],
                                    likes_week_ago=(df[df['date'] == today.date()]['likes'].iloc[0]
                                                           - df[df['date'] == week_ago.date()]['likes'].iloc[0])
                                                           /df[df['date'] == week_ago.date()]['likes'].iloc[0],

                                    views=df[df['date'] == today.date()]['views'].iloc[0],
                                    views_day_ago=(df[df['date'] == today.date()]['views'].iloc[0]
                                                           - df[df['date'] == day_ago.date()]['views'].iloc[0])
                                                           /df[df['date'] == day_ago.date()]['views'].iloc[0],
                                    views_week_ago=(df[df['date'] == today.date()]['views'].iloc[0]
                                                           - df[df['date'] == week_ago.date()]['views'].iloc[0])
                                                           /df[df['date'] == week_ago.date()]['views'].iloc[0],

                                    ctr=df[df['date'] == today.date()]['CTR'].iloc[0],
                                    ctr_day_ago=(df[df['date'] == today.date()]['CTR'].iloc[0]
                                                           - df[df['date'] == day_ago.date()]['CTR'].iloc[0])
                                                           /df[df['date'] == day_ago.date()]['CTR'].iloc[0],
                                    ctr_week_ago=(df[df['date'] == today.date()]['CTR'].iloc[0]
                                                           - df[df['date'] == week_ago.date()]['CTR'].iloc[0])
                                                           /df[df['date'] == week_ago.date()]['CTR'].iloc[0]
                                        )

                return report

     
            #Function for a graph with metrics for the week.
            @task()
            def load_report2(df):
                #settings for the graph
                fig, axes = plt.subplots(2, 2, figsize=(12,6))
                plt.suptitle('Метрики за прошлую неделю')
                x = df.date.dt.day


                axes[0, 0].set_title('DAU')
                axes[0, 1].set_title('CTR')
                axes[1, 0].set_title('Likes')
                axes[1, 1].set_title('Views')


                sns.lineplot(x=x, y=df.DAU, ax=axes[0, 0], color='red').set(xlabel=None, ylabel=None) 
                sns.lineplot(x=x, y=df.CTR, ax=axes[0, 1], color='red').set(xlabel=None, ylabel=None) 
                sns.lineplot(x=x, y=df.likes, ax=axes[1, 0], color='red').set(xlabel=None, ylabel=None) 
                sns.lineplot(x=x, y=df.views, ax=axes[1, 1], color='red').set(xlabel=None, ylabel=None)

                #saving the graph
                plot_object = io.BytesIO()
                plt.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = 'DAU.png'
                plt.close()

                return plot_object
            #sending a report
            @task()
            def sent_report(chat_id, report, plot_object):
                bot.sendMessage(chat_id=715361296, text=report)
                bot.sendPhoto(chat_id=715361296, photo=plot_object)

            df = get_feeds()
            report=load_report1(df)
            plot_object=load_report2(df)
            sent_report=sent_report(chat_id, report, plot_object)

krllb_tg_report=krllb_tg_report()    