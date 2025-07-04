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
from print_manager import PrintManager
pm = PrintManager()

from llama_index.core.response.pprint_utils import pprint_response
from llama_index.llms.openai import OpenAI
from llama_index.core import Settings
from llama_index.core.utilities.sql_wrapper import SQLDatabase
from llama_index.core.query_engine import NLSQLTableQueryEngine
from llama_index.core.workflow import (
    Event,
    StartEvent,
    StopEvent,
    Workflow,
    step,
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
        super().__init__()
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

async def analyze_intent(self, prompt: str)->tuple[str, str]:
    """Analyze the purpose of the user's prompt"""
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
    
    Only write "SQL" or "CHAT".
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

class SQLAnalysisAgent(Workflow):
    def __init__(self):
        super().__init__()
        self.llm = OpenAI()
        Settings.llm = self.llm

        # logging settings
        log_file = f"logs/sql_agent{datetime.now().strftime("%Y%m%d")}.log"
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
        self.db_connection = sqlite3.connect(db_path, check_same_thread=True)
        self.cursor = self.db_connection.cursor()

        # safe SQL patterns
        self.safe_patterns = {
            'SELECT': r'^SELECT\s+(?:(?:[\w\s,.()*]|\s)+)\s+FROM\s+[\w]+(?:\s+WHERE\s+[\w\s><=]+)?(?:\s+ORDER\s+BY\s+[\w\s,]+)?(?:\s+LIMIT\s+\d+)?$',
            'COUNT': r'^SELECT\s+COUNT\s*\(\s*\*\s*\)\s+FROM\s+[\w]+(?:\s+WHERE\s+[\w\s><=]+)?$',
            'AVG': r'^SELECT\s+AVG\s*\(\s*[\w]+\s*\)\s+FROM\s+[\w]+(?:\s+WHERE\s+[\w\s><=]+)?$'
        }

        # unsafe characters and patterns
        self.dangerous = {
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
        self.allowed_tables = {"product"}
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

def analyze_prompt_safety(self, prompt: str) ->tuple[bool, str]:
    """Analyze the user's prompt and check it's safety"""
    if not prompt or not prompt.strip():
        return False, "Empty query"

    for pattern in self.malicious_prompts_patterns:     # or you can check it with dangerous_patterns as welL!
        if re.search(pattern, prompt):
            return False, f"Malicious pattern detected: {pattern}"

    safe_pattern_found = any(re.search(pattern, prompt) for pattern in self.safe_prompt_patterns)
    if not self.safe_prompt_patterns:
        return False, "Query does not contain safe patterns"

    if len(prompt) > 500:
        return False, "Query too long"

    return True, "Query is safe"


async def verify_prompt_with_llm(self, prompt: str) ->tuple[bool, str]:
    """Verify the safety of prmpt using LLM"""
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




