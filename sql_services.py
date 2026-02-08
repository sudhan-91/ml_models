import json
import os
import pandas as pd
import traceback
import sqlalchemy
import pymssql
from psycopg2 import pool
from sqlalchemy import Column, Integer, String, Text, DateTime, insert, Boolean, text
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func
from contextlib import closing
base_dir = os.getcwd()
config = json.load(open("configs/global_config.json"))
dive_config = config['dive_config']
sql_config = config['sql_server_config']
Base = declarative_base()


class ChatMessage(Base):
    __tablename__ = "message"

    message_id = Column(String(36), primary_key=True)
    chat_id = Column(String(36), nullable=False, index=True)
    user_id = Column(String(50), nullable=True)
    message = Column(Text, nullable=False)
    response = Column(Text, nullable=True)
    vote = Column(Text, nullable=True)
    feedback = Column(Text, nullable=True)
    latency_ms = Column(Integer, nullable=True)
    topic_id = Column(String(36), nullable=False, index=True)
    metadata_json = Column(Text, nullable=True)
    created_time = Column(DateTime, server_default=func.now(), nullable=False)
    modified_time = Column(DateTime, nullable=True)
    deleted_time = Column(DateTime, nullable=True)
    folder_id = Column(Text, nullable=True)
    file_name = Column(Text, nullable=True)

class Session(Base):
    __tablename__ = "session"

    chat_id = Column(String(36), primary_key=True, index=True)
    user_id = Column(String(50), nullable=True)
    chat_history = Column(Text, nullable=False)
    title = Column(Text, nullable=False)
    bookmark_status = Column(Boolean, nullable=False, default=False)
    created_time = Column(DateTime, server_default=func.now(), nullable=False)
    modified_time = Column(DateTime, nullable=True)
    deleted_time = Column(DateTime, nullable=True)
    topic_id = Column(String(36), nullable=False, index=True)

class Prompt(Base):
    __tablename__ = "prompt"

    prompt_id = Column(String(36), primary_key=True, index=True)
    prompt = Column(Text, nullable=False)
    topic_id = Column(String(36), nullable=False, index=True)
    created_time = Column(DateTime, server_default=func.now(), nullable=False)
    modified_time = Column(DateTime, nullable=True)
    deleted_time = Column(DateTime, nullable=True)


class sqlServices:
    """Postgres query service using LoadPrerequisite instance with context management."""
    def __init__(self):
        try:
            self.intialize_mssql()

            dive_str = "user=%s password=%s host=%s dbname=%s port=%s" % \
                       (dive_config['username'],dive_config['password'],dive_config['host'],
                        dive_config['service_name'], dive_config['port'])

            self.dive_conn_pool = pool.ThreadedConnectionPool(1, 10, dsn=dive_str)
            self.db_connection = self.dive_conn_pool.getconn()
            self.retry_interval = 2
            self.max_retry_time = 6
        except Exception:
            print(f'Error in Init {traceback.print_exc()}')

    def intialize_mssql(self):
        try:

            # far_connection_url = f'mssql+pymssql://{sql_config["domain"]}\{sql_config["username"]}:{sql_config["password"]}@{sql_config["host"]}:{sql_config["port"]}/{sql_config["qualitics_database"]}'
            # self.far_mssql_engine = sqlalchemy.create_engine(far_connection_url)

            connection_url = f'mssql+pymssql://{sql_config["domain"]}\{sql_config["username"]}:{sql_config["password"]}@{sql_config["host"]}:{sql_config["port"]}/{sql_config["qualitics_database"]}'
            # Local Testing
            # db_name = "qmchatbot_dev"
            # connection_url = f"mssql+pyodbc://(localdb)\\MSSQLLocalDB/qmchatbot_dev?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
            self.mssql_engine = sqlalchemy.create_engine(connection_url)
            Base.metadata.create_all(self.mssql_engine)
            print("Tables created successfully!")

        except:
            print(f'{traceback.print_exc()}')

    def select_query_func(self, query):
        df = ""
        try:
            if "select" in query.lower():
                with closing(self.db_connection.cursor()) as cursor:
                    cursor.execute(query)
                    records = cursor.fetchall()
                    column_names = [desc[0] for desc in cursor.description]
                    df = pd.DataFrame(records, columns=column_names)
        except Exception:
            print(f'Error in select_query_func {traceback.print_exc()}')
            self.dive_conn_pool.putconn(self.db_connection, close=False)
        return df

    def select_qualitics_db(self, query):
        df = ""
        try:
            if "select" in query.lower():
                df = pd.read_sql(query, self.mssql_engine)

        except Exception:
            print(f'Error in select_qualitics_db {traceback.print_exc()}')
        return df

    def select_user_info(self, query, condition):
        records = {}
        try:
            with self.mssql_engine.begin() as conn:
                result = conn.execute(text(query), condition)
                records = result.mappings().all()
        except Exception:
            print(f'Error in select_qualitics_db {traceback.print_exc()}')
        return records

    def insert_session_data(self, session_table_data):
        try:
            with self.mssql_engine.begin() as conn:
                conn.execute(
                    insert(Session).values(
                        chat_id= session_table_data['chat_id'],
                        user_id= session_table_data['user_id'],
                        chat_history = "",
                        title=session_table_data['input_message'],
                        topic_id=session_table_data['topic_id'],
                        modified_time = "",
                        deleted_time = "",
                    )
                )
        except:
            pass

    def insert_chat_history(self, message_table_data):
        try:
            with self.mssql_engine.begin() as conn:
                conn.execute(
                    insert(ChatMessage).values(
                        message_id=message_table_data['message_id'],
                        chat_id=message_table_data['chat_id'],
                        user_id=message_table_data['user_id'],
                        message=message_table_data['input_message'],
                        response=message_table_data['model_response'],
                        vote=message_table_data['vote'],
                        feedback=message_table_data['feedback'],
                        latency_ms=message_table_data['latency'],
                        topic_id=message_table_data['topic_id'],
                        metadata_json=message_table_data['meta_data'],
                        modified_time="",
                        deleted_time="",
                        folder_id="",
                        file_name="",
                    )
                )
        except:
            print(f'Error in inserting the record {traceback.print_exc()}')

    def insert_prompt_data(self, prompt_table_data):
        try:
            with self.mssql_engine.begin() as conn:
                conn.execute(
                    insert(Prompt).values(
                        prompt_id=prompt_table_data['prompt_id'],
                        prompt = prompt_table_data['prompt'],
                        topic_id=prompt_table_data['topic_id'],
                        modified_time="",
                        deleted_time="",
                    )
                )
        except:
            print(f'Error in inserting the record {traceback.print_exc()}')

    def execute_update(self, query, condition):
        try:
            with self.mssql_engine.begin() as conn:
                conn.execute(text(query), condition)
        except Exception:
             print(f'Error in execute_update {traceback.print_exc()}')