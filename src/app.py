from dotenv import load_dotenv
import streamlit as st
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_community.utilities import SQLDatabase
from langchain_core.output_parsers import StrOutputParser
from langchain_openai import ChatOpenAI
import os

# Initialize environment variables
load_dotenv()

# Set Streamlit page configuration
st.set_page_config(page_title="Chat with MySQL", page_icon=":speech_balloon:")

# Database connection settings (extracted from environment variables for security)
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_DATABASE = os.getenv("DB_DATABASE")


# Function to initialize database connection
@st.cache_resource
def init_database(user: str, password: str, host: str, port: str, database: str) -> SQLDatabase:
    db_uri = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}"
    return SQLDatabase.from_uri(db_uri)

# Attempt to establish the database connection
db = init_database(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_DATABASE)
if db is not None:
    st.success("Connected to database!")

def get_sql_chain(db):
    template = """
    You are a data analyst at a tourism-focused publication. You interact with users inquiring about various aspects of tourism articles and related data stored in your database. The database structure includes two principal tables: column_names and article_data. The column_names table stores details about each column from the original Excel file, including a unique ID for each column (id), the column's name (column_name), and its position in the Excel file represented as a letter (excel_column_position). The article_data table is designed to capture the data for each tourism article, linking every piece of data to its corresponding column through the column_id field, which references the id in the column_names table.

    When users mention columns by their letter identifiers in inquiries, your replies should interpret these letters into the specific aspects of tourism articles that these columns represent. This conversion from column letters to their meaningful content guarantees that your responses are articulated in an understandable, natural language.

    Given the table schema, you'll be crafting SQL queries to fetch information as per user queries, considering the conversation history for context. The goal is to leverage the database structure to provide meaningful insights into the tourism data captured from various articles, focusing on the nuanced details that make each piece of data significant.

    <SCHEMA>{schema}</SCHEMA>

    Conversation History: {chat_history}

    Write only the SQL query and nothing else. Do not wrap the SQL query in any other text, not even backticks.

    For example:
    Question: which 3 artists have the most tracks?
    SQL Query: SELECT ArtistId, COUNT(*) as track_count FROM Track GROUP BY ArtistId ORDER BY track_count DESC LIMIT 3;
    Question: Name 10 artists
    SQL Query: SELECT Name FROM Artist LIMIT 10;

    Your turn:

    Question: {question}
    SQL Query:
    """
    prompt = ChatPromptTemplate.from_template(template)
    llm = ChatOpenAI(model="gpt-4-turbo-preview")
    # llm = ChatOpenAI(model="gpt-3.5-turbo")
    # llm = ChatGroq(model="mixtral-8x7b-32768", temperature=0)

    def get_schema(_):
        return db.get_table_info()

    return (
        RunnablePassthrough.assign(schema=get_schema)
        | prompt
        | llm
        | StrOutputParser()
    )

def get_response(user_query: str, db: SQLDatabase, chat_history: list):
    sql_chain = get_sql_chain(db)
    template = """
    You are a data analyst at a company. You are interacting with a user who is asking you questions about the company's database.
    Based on the table schema below, question, sql query, and sql response, write a natural language response in spanish.
    <SCHEMA>{schema}</SCHEMA>

    Conversation History: {chat_history}
    SQL Query: <SQL>{query}</SQL>
    User question: {question}
    SQL Response: {response}"""

    prompt = ChatPromptTemplate.from_template(template)
    llm = ChatOpenAI(model="gpt-4-turbo-preview")

    chain = (
        RunnablePassthrough.assign(query=sql_chain).assign(
            schema=lambda _: db.get_table_info(),
            response=lambda vars: db.run(vars["query"]),
        )
        | prompt
        | llm
        | StrOutputParser()
    )

    return chain.invoke({
        "question": user_query,
        "chat_history": chat_history,
    })

if "chat_history" not in st.session_state:
    st.session_state.chat_history = [
        AIMessage(content="Hello! I'm a SQL assistant. Ask me anything about your database."),
    ]

st.title("Chat with MySQL")

# Chat interface to display messages and handle user input
for message in st.session_state.chat_history:
    if isinstance(message, AIMessage):
        with st.chat_message("AI"):
            st.markdown(message.content)
    elif isinstance(message, HumanMessage):
        with st.chat_message("Human"):
            st.markdown(message.content)

user_query = st.chat_input("Type a message...")
if user_query is not None and user_query.strip() != "":
    st.session_state.chat_history.append(HumanMessage(content=user_query))

    with st.chat_message("Human"):
        st.markdown(user_query)

    response = get_response(user_query, db, st.session_state.chat_history)
    
    with st.chat_message("AI"):
        st.markdown(response)

    st.session_state.chat_history.append(AIMessage(content=response))