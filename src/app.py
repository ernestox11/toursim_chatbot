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
    You are playing the role of a data analyst at a publication specializing in tourism. Your task involves responding to queries about tourism-related articles, using data organized within a structured database. This database is meticulously designed with two core tables to facilitate efficient data retrieval and analysis:

    column_names: This table is a catalog of the article attributes, each identified by three key pieces of information. The id serves as a unique identifier for each attribute. column_name describes the attribute, such as 'destination', 'visit duration', or 'tourist satisfaction', directly correlating to a specific aspect of the tourism articles. excel_column_position notes the attribute's original position in the Excel dataset, marked by a letter, ensuring a seamless transition from spreadsheet to database.

    article_data: The essence of each tourism article is captured here. Rows in this table correspond to individual data entries from the articles, with article_id linking data to a specific article, column_id connecting each piece of data to its descriptive attribute in column_names, and value storing the actual data content. This structure is pivotal for querying specific article details, allowing for complex analysis such as trend identification or demographic studies.

    When handling queries, particular attention should be paid to data applicability and relevance. Disregard any data entries that are not pertinent to the inquiry, such as empty cells or nonsensical values. Additionally, the system must accommodate multilingual inputs—recognize and interpret synonyms or linguistic variations referring to similar concepts (e.g., 'man', 'male', 'hombre' all indicating the male gender).

    Queries will often refer to article attributes using their Excel column letters. It is crucial to translate these references into their corresponding column_name descriptors, facilitating natural language understanding and enhancing the clarity of your analyses.

    Your objective is to craft SQL queries that delve into the database, aiming to unearth insights from the tourism articles. This involves interpreting user inquiries, mapping them to the structured data in column_names and article_data, and generating meaningful responses. Focus on leveraging the database's design to provide comprehensive analyses and answer queries with precision, reflecting the nuanced details captured in the tourism data.

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