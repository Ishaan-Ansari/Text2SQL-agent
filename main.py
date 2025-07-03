import traceback
from typing import Optional
import streamlit as st
import os
import os.path

from llama_index.core.response.pprint_utils import pprint_response
from llama_index.llms.openai import OpenAI
from llama_index.core.utilities.sql_wrapper import SQLDatabase
from llama_index.core.service_context import ServiceContext
from llama_index.core.query_engine import NLSQLTableQueryEngine

from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    String,
    Integer,
    select,
    text
)

from dotenv import load_dotenv
load_dotenv()


@st.cache_resource
def init_database_connection():
    """initialize database connection"""
    try:
        required_vars = ['DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_NAME']
        missing_vars = [var for var in required_vars if not os.getenv(var)]

        if missing_vars:
            st.error(f"Missing .env variables: {', '.join(missing_vars)}")
            return None, None, None

        db_url = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"

        # create engine with connection pooling
        engine = create_engine(
            db_url,
            pool_pre_ping=True,
            pool_recycle=300,
            echo=False,
        )

        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        # initialize LLM
        llm = OpenAI(
            temperature=0.1,
            model="gpt-4-turbo"
        )

        # create service context
        service_context = ServiceContext.from_defaults(llm=llm)

        # initialize SQL database wrapper
        sql_database = SQLDatabase(
            engine,
            include_tables=["product_master", "inventory"]
        )

        # create query engine
        query_engine = NLSQLTableQueryEngine(
            sql_database=sql_database,
            tables=["product_master", "inventory"],
            verbose=True
        )

        return engine, sql_database, query_engine

    except Exception as e:
        st.error(f"Failed to initialize database connection: {str(e)}")
        return None, None, None

@st.cache_data
def get_table_info(_sql_database):
    """Get table schema information for display"""
    try:
        table_info = {}
        for table_name in ["product_master", "inventory"]:
            table_info[table_name] = _sql_database.get_table_info(table_name)
        return table_info
    except Exception as e:
        st.error(f"Failed to get table info: {str(e)}")
        return {}

def queryDB(query_str: str, query_engine) -> Optional[dict]:
    """Query the databse with error handling"""
    try:
        if not query_str.strip():
            return {"error": "Please enter a valid query"}
        response = query_engine.query(query_str)
        return {
            "success": True,
            "response": response.response,
            "sql_query": getattr(response, 'metadata', {}).get('sql_query', 'N/A'),
            "full_response": response
        }

    except Exception as e:
        error_msg = f"Query failed: {str(e)}"
        st.error(error_msg)
        return {
            "success": False,
            "error": error_msg,
            "traceback": traceback.format_exc()
        }

def display_chat_history():
    """Display chat history"""
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.write(message["content"])

            if message["role"] == "assistant" and "sql_query" in message:
                with st.expander("Generated SQL Query"):
                    st.code(message["sql_query"], language="sql")


def main():
    pass

