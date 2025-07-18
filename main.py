import logging
import traceback
from typing import Optional, List, Tuple, Dict, Any
from contextlib import contextmanager
import streamlit as st
import os
from pathlib import Path
import sqlite3
import datetime
import re
import time
import asyncio
from print_manager import PrintManager

pm = PrintManager()

from llama_index.core.response.pprint_utils import pprint_response
from llama_index.llms.openai import OpenAI
from llama_index.core import Settings
from llama_index.core.utilities.sql_wrapper import SQLDatabase
from llama_index.core.query_engine import NLSQLTableQueryEngine
from llama_index.core.workflow import (
    Event,  # Base class for all events
    StartEvent,  # signals start of the workflow
    StopEvent,
    Workflow,
    step,  # Decorator used to register methods as workflow steps
)

from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()


class IntentAnalysisEvent(Event):
    intent: str
    message: str


class SQLGenerationEvent(Event):
    sql_query: str


class SQLExecutionEvent(Event):
    execution_result: str
    execution_time: float
    row_count: int


class FeedbackEvent(Event):
    feedback: str
    success: bool


class IntentAnalyzer(Workflow):
    def __init__(self):
        super().__init__()  # Initialize the base Workflow machinery, [Let's u call the base class without naming it!]
        self.llm = OpenAI()
        Settings.llm = self.llm

        # Defined intent patterns
        self.sql_patterns = [
            r'(?i)(show|list|find|search|sort)',
            r'(?i)(products|stock|price)',
            r'(?i)(how many|total|average)',
            r'(?i)(sql|query|database)',
            r'(?i)(highest|lowest|maximum|minimum)',
        ]

        self.chat_patterns = [
            r'(?i)(hello|hi|how are you)',
            r'(?i)(chat|talk|conversation)',
            r'(?i)(what are you doing|who are you)',
            r'(?i)(thank you|thanks)',
        ]

    async def analyze_intent(self, prompt: str) -> tuple[str, str]:
        """Analyze the purpose of the user's prompt"""
        if not prompt or not prompt.strip():
            return "chat", "Empty prompt detected"
        if any(re.search(pattern, prompt) for pattern in self.sql_patterns):
            return "sql", "SQL query detected"
        if any(re.search(pattern, prompt) for pattern in self.chat_patterns):
            return "chat", "Chat intent detected"

        # Detailed analysis with LLM
        analysis_prompt = f"""
        Please analyze the purpose of the following user message:
        "{prompt}"
        
        There are only two options:
        1. SQL: The user wants to perform a database query
        2. CHAT: The user wants to chat
                
        Consider these factors:
        - Does it mention products, prices, stock, or database operations?
        - Is it asking for data retrieval or analysis?
        - Is it a greeting or general conversation?
        
        Respond only with "SQL" or "CHAT".
        """

        response = await self.llm.acomplete(analysis_prompt)
        intent = str(response).strip().upper()

        if intent == "SQL":
            return "sql", "LLM analysis, SQL query detected"
        return "chat", "LLM analysis: chat intent detected"

    @step
    async def determine_intent(self, ev: StartEvent) -> StopEvent:
        prompt = ev.topic
        intent, message = await self.analyze_intent(prompt)
        return StopEvent(result={'intent': intent, "message": message})


class SQLAnalysisAgent(Workflow):  # I want all the machinery that workflows provide plus my own addition
    def __init__(self):
        super().__init__()
        self.llm = OpenAI(timeout=150.0)
        Settings.llm = self.llm

        # logging settings
        log_file = f"logs/sql_agent{datetime.datetime.now().strftime('%Y%m%d')}.log"
        os.makedirs('logs', exist_ok=True)
        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

        # SQLite connection
        db_path = Path('data/database.db')
        if not db_path.parent.exists():
            db_path.parent.mkdir(parents=True)
        self.db_connection = sqlite3.connect(db_path, check_same_thread=False)  # False for multi-threading support
        self.cursor = self.db_connection.cursor()

        # safe SQL patterns
        self.safe_patterns = {
            'SELECT': r'^SELECT\s+(?:(?:[\w\s,.()*]|\s)+)\s+FROM\s+[\w]+(?:\s+WHERE\s+[\w\s><=]+)?(?:\s+ORDER\s+BY\s+[\w\s,]+)?(?:\s+LIMIT\s+\d+)?$',
            'COUNT': r'^SELECT\s+COUNT\s*\(\s*\*\s*\)\s+FROM\s+[\w]+(?:\s+WHERE\s+[\w\s><=]+)?$',
            'AVG': r'^SELECT\s+AVG\s*\(\s*[\w]+\s*\)\s+FROM\s+[\w]+(?:\s+WHERE\s+[\w\s><=]+)?$',
            'MAX': r'^SELECT\s+MAX\s*\(\s*[\w]+\s*\)\s+FROM\s+[\w]+(?:\s+WHERE\s+[\w\s><=\'\"]+)?$',
            'MIN': r'^SELECT\s+MIN\s*\(\s*[\w]+\s*\)\s+FROM\s+[\w]+(?:\s+WHERE\s+[\w\s><=\'\"]+)?$',
            'SUM': r'^SELECT\s+SUM\s*\(\s*[\w]+\s*\)\s+FROM\s+[\w]+(?:\s+WHERE\s+[\w\s><=\'\"]+)?$',
        }

        # unsafe characters and patterns
        self.dangerous_patterns  = {
            r';.*',  # Multiple queries
            r'--.*',  # SQL comments
            r'/\*.*?\*/',  # Multiline comments
            r'xp_.*',  # System stored procedures
            r'exec.*',  # Execute commands
            r'UNION.*',  # UNION attacks
            r'DROP.*',  # DROP commands
            r'DELETE.*',  # DELETE commands
            r'UPDATE.*',  # UPDATE commands
            r'ALTER.*',  # ALTER commands
            r'TRUNCATE.*',  # TRUNCATE commands
            r'INSERT.*',  # INSERT commands
            r'GRANT.*',  # GRANT commands
            r'REVOKE.*',  # REVOKE commands
            r'SYSTEM.*',  # System commands
            r'INTO\s+(?:OUTFILE|DUMPFILE).*',  # File operations
        }

        # Allowed tables and columns
        self.allowed_tables = {"products"}
        self.allowed_columns = {
            'products': {'id', 'name', 'price', 'stock'}
        }

        # Malicious prompts patterns
        self.malicious_prompts_patterns = [
            r'(?i)(drop|delete|truncate|alter)\s+table',  # Dropping/modifying tables
            r'(?i)system\s+command',  # System commands
            r'(?i)(hack|exploit|attack)',  # Malicious words
            r'(?i)(union\s+select|join\s+select)',  # SQL injection
            r'(?i)(--|;|/\*|\*/)',  # SQL comments and separators
            r'(?i)(xp_cmdshell|exec\s+sp)',  # Stored procedures
            r'(?i)(insert\s+into|update\s+set)',  # Data modification
            r'(?i)password|username|credential',  # Sensitive data
            r'(?i)grant|revoke|permission',  # Permission changes
            r'(?i)backup|restore|dump',  # Backup operations
        ]

        self.safe_prompt_patterns = [
            r'(?i)(show|list|find|search|sort)',
            r'(?i)(products|stock|price)',
            r'(?i)(how many|total|average)',
            r'(?i)(highest|lowest|maximum|minimum)',
        ]

    def analyze_prompt_safety(self, prompt: str) -> tuple[bool, str]:
        """Analyze the user's prompt and check it's safety"""
        if not prompt or not prompt.strip():
            return False, "Empty query"

        for pattern in self.malicious_prompts_patterns:  # or you can check it with dangerous_patterns as welL!
            if re.search(pattern, prompt):
                return False, f"Malicious pattern detected: {pattern}"

        safe_pattern_found = any(re.search(pattern, prompt) for pattern in self.safe_prompt_patterns)
        if not safe_pattern_found:
            return False, "Query does not contain safe patterns"

        if len(prompt) > 500:
            return False, "Query too long"

        return True, "Query is safe"

    async def verify_prompt_with_llm(self, prompt: str) -> tuple[bool, str]:
        """Verify the safety of prompt using LLM"""
        verification_prompt = f"""
        Please analyze the safety of the following user query:
        "{prompt}"
        
        Check the following:
        1. Is there any attempt of SQL injection?
        2. Does it contain malicious commands?
        3. Are there any statements that threaten system security? Guardrail it
        4. Is it a query solely for data reading purposes?
        
        Only write "SAFE" or "UNSAFE" and briefly state the reason.
        """
        response = await self.llm.acomplete(verification_prompt)
        result = str(response).strip().upper()

        is_safe = result.startswith("SAFE")
        message = result.replace("SAFE", "").replace("UNSAFE", "").strip()

        return is_safe, message

    def sanitize_input(self, value: str) -> str:
        """Sanitize input against SQL injection"""
        if value is None:
            return None
        return value.replace("'", "''").replace(";", "").replace("--", "")

    def validate_sql_safety(self, sql_query: str) -> tuple[bool, str]:
        """Check the safety of SQL query"""
        if not sql_query or not sql_query.strip():
            return False, "Empty query"

        sql_query = sql_query.upper()

        if not sql_query.strip().startswith('SELECT'):
            return False, "Only SELECT queries are allowed"

        for pattern in self.dangerous_patterns:
            if re.search(pattern, sql_query, re.IGNORECASE):
                return False, f"Dangerous pattern detected: {pattern}"

        tables = re.findall(r'FROM\s+(\w+)', sql_query, re.IGNORECASE)
        for table in tables:
            if table.lower() not in self.allowed_tables:
                return False, f"Accessing unauthorized table: {table}"

        for pattern in self.safe_patterns.values():
            if re.match(pattern, sql_query, re.IGNORECASE):
                return True, "Safe query"

        return False, "Unsuitable query format"

    def format_results(self, results, description):
        """Format query results"""
        if not results:
            return "No results found"

        column_names = [desc[0] for desc in description]
        formatted_results = "\nQuery Results:\n"
        formatted_results += "-" * 80 + "\n"
        formatted_results += " | ".join(f"{col:15}" for col in column_names) + "\n"
        formatted_results += "-" * 80 + "\n"

        for row in results:
            formatted_results += " | ".join(f"{str(item):15}" for item in row) + "\n"
        formatted_results += "-" * 80 + "\n"

        return formatted_results

    def learn_from_history(self, natural_query: str) -> str:
        """Learn from past queries"""
        self.cursor.execute("""
                SELECT natural_query, generated_sql, execution_result 
                FROM query_history 
                WHERE natural_query LIKE ? 
                AND execution_result NOT LIKE '%error%'
                ORDER BY created_at DESC 
                LIMIT 1
        """, (f"%{natural_query}%",))

        similar_query = self.cursor.fetchone()
        if similar_query:
            pm.info(f"\nSimilar query found: \n{similar_query}")
            return similar_query[1]
        return None

    def log_error(self, error_msg: str, query: str):
        """Log security breaches and errors"""
        logging.error(f"Security breach - Query: {query}\n Error: {error_msg}")
        try:
            self.cursor.execute("""
                INSERT INTO error_stats (error_type, query, message)
                VALUES (?, ?, ?)
            """, ('SECURITY_VIOLATION', query, error_msg))
            self.db_connection.commit()
        except Exception as e:
            logging.error(f"Log entry error: {str(e)}")

    @step
    async def generate_sql(self, ev: StartEvent) -> SQLGenerationEvent:
        prompt = ev.topic

        # Security checks
        is_safe, message = self.analyze_prompt_safety(prompt)

        if not is_safe:
            pm.security(f"Prompt is not safe {message}", False)
            return SQLGenerationEvent(sql_query="SELECT 'Failed security check' as message")

        pm.security("Security checks passed", True)

        # Generate SQL
        pm.subsection('SQL generation')
        query = self.sanitize_input(prompt)

        # Get schema info.
        self.cursor.execute("SELECT table_name, columns_info FROM tables_info")
        schema_info = self.cursor.fetchall()

        # Learn from history
        learned_sql = self.learn_from_history(query)

        if learned_sql:
            pm.info(f"SQL learned from history: {learned_sql}")
            is_safe, message = self.validate_sql_safety(learned_sql)
            if is_safe:
                return SQLGenerationEvent(sql_query=learned_sql)

        # Generate SQL with LLM
        sql_prompt = f"""
        Database schema:
        {schema_info}

        Please translate the following natural language query into an SQL query:
        {query}

        IMPORTANT RULES:
        1. Only SELECT queries are allowed
        2. Only access the 'products' table
        3. Allowed columns: id, name, price, stock
        4. No multiple queries, comments, or special characters
        5. No complex queries like UNION, JOIN

        Only return the SQL query.
        """

        response = await self.llm.acomplete(sql_prompt)
        sql_query = str(response).strip().replace('```sql', '').replace('```', '').strip()
        sql_query = sql_query.rstrip(';')

        # validate SQL query
        is_safe, message = self.validate_sql_safety(sql_query)

        if not is_safe:
            pm.security(f"Generated SQL is not safe: {message}", False)
            return SQLGenerationEvent(sql_query="SELECT 'Security breach detected' as message")

        pm.success("SQL query generated successfully")
        pm.info(f"Original Query: {query}")
        pm.info(f"Generated SQL: {sql_query}")

        # Save SQL query
        self.cursor.execute(
            "INSERT INTO query_history (natural_query, generated_sql) VALUES (?, ?)",
            (query, sql_query)
        )
        self.db_connection.commit()

        return SQLGenerationEvent(sql_query=sql_query)

    @step
    async def execute_sql(self, ev: SQLGenerationEvent) -> SQLExecutionEvent:
        sql_query = ev.sql_query
        try:
            # Execute query
            start_time = time.time()
            self.cursor.execute(sql_query)
            result = self.cursor.fetchall()
            execution_time = time.time() - start_time

            # format results
            formatted_results = self.format_results(result, self.cursor.description)
            print(formatted_results)

            return SQLExecutionEvent(
                execution_result=formatted_results,
                execution_time=execution_time,
                row_count=len(result)
            )

        except Exception as e:
            error_message = f"Error in executing SQL query: {str(e)}"
            self.log_error(str(e), error_message)
            return SQLExecutionEvent(
                execution_result=error_message,
                execution_time=0,
                row_count=0
            )

    def create_feedback_prompt(self, ev: SQLExecutionEvent) -> str:
        """Create a feedback prompt for the LLM"""
        return f"""
        Please briefly evaluate the result of this query:
        
        Query Metrics:
        - Execution Time: {ev.execution_time:.4f} seconds
        - Rows Returned: {ev.row_count}
        
        Query Result:
        {ev.execution_result}
        
        Please provide a SHORT and CLEAR evaluation based on the following criteria:
        1. Was the query successful? (Yes/No)
        2. Is the performance adequate? (Yes/No)
        3. If any, what are the improvement suggestions?
        """

    @step
    async def collect_feedback(self, ev: SQLExecutionEvent) -> StopEvent:
        feedback_prompt = self.create_feedback_prompt(ev)
        feedback = await self.llm.acomplete(feedback_prompt)
        print(str(feedback))
        return StopEvent(result=str(ev.execution_result))

    # 'destructor' - clean up external resources, closing db, and connection
    def __del__(self):
        """Cleanup operations"""
        try:
            if hasattr(self, 'cursor'):
                self.cursor.close()
            if hasattr(self, 'db_connection'):
                self.db_connection.close()
        except Exception as e:
            logging.error(f"Cleanup error: {str(e)}")


async def run_sql_agent(natural_query: str) -> str:
    """Run the SQL analysis agent"""
    intent_analyzer = IntentAnalyzer()
    result_dict = await intent_analyzer.run(topic=natural_query)  ## directly det the dictionary

    if result_dict["intent"] == "chat":
        pm.warning("I can only help with database")
        return "Please ask question relevant to a Db query"

    # If it's an SQL query with a normal flow
    agent = SQLAnalysisAgent()
    result = await agent.run(topic=natural_query, timeout=150)
    return str(result)


async def main():
    natural_query = "Give me the price of the most expensive product"
    result = await run_sql_agent(natural_query)
    print(result)


if __name__ == '__main__':
    asyncio.run(main())
