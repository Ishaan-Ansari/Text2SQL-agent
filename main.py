import traceback
from typing import Optional, List, Tuple, Dict, Any
from contextlib import contextmanager
import streamlit as st
import os
import sqlite3
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


