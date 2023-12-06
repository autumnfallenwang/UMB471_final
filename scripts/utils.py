import os
import pyodbc
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database configuration dictionary
db_config = {
    'server': os.getenv('DB_SERVER'),
    'port': os.getenv('DB_PORT'),
    'username': os.getenv('DB_USERNAME'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME')
}

# Global variable for the connection string
CONN_STRING = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={db_config["server"]},{db_config["port"]};DATABASE={db_config["database"]};UID={db_config["username"]};PWD={db_config["password"]};autocommit=True'

# Complete dimension to table mapping
dim_table_map = {
    # Primary dimensions
    'year': ('DimDate', 'Fact_MovieData.dateKey = DimDate.dateKey', 'primary'),
    'titleType': ('DimMovie', 'Fact_MovieData.movieId = DimMovie.movieId', 'primary'),
    'primaryTitle': ('DimMovie', 'Fact_MovieData.movieId = DimMovie.movieId', 'primary'),
    'originalTitle': ('DimMovie', 'Fact_MovieData.movieId = DimMovie.movieId', 'primary'),
    'isAdult': ('DimMovie', 'Fact_MovieData.movieId = DimMovie.movieId', 'primary'),
    'startYear': ('DimMovie', 'Fact_MovieData.movieId = DimMovie.movieId', 'primary'),
    'endYear': ('DimMovie', 'Fact_MovieData.movieId = DimMovie.movieId', 'primary'),
    'runtimeMinutes': ('DimMovie', 'Fact_MovieData.movieId = DimMovie.movieId', 'primary'),

    # Secondary dimensions requiring bridge tables
    'genreName': ('DimGenre', 'Bridge_MovieGenres.genreId = DimGenre.genreId', 'secondary', 'Bridge_MovieGenres', 'Fact_MovieData.movieId = Bridge_MovieGenres.movieId'),
    'name': ('DimPerson', 'Bridge_MoviePrincipals.personId = DimPerson.personId', 'secondary', 'Bridge_MoviePrincipals', 'Fact_MovieData.movieId = Bridge_MoviePrincipals.movieId'),
    'birthYear': ('DimPerson', 'Bridge_MoviePrincipals.personId = DimPerson.personId', 'secondary', 'Bridge_MoviePrincipals', 'Fact_MovieData.movieId = Bridge_MoviePrincipals.movieId'),
    'deathYear': ('DimPerson', 'Bridge_MoviePrincipals.personId = DimPerson.personId', 'secondary', 'Bridge_MoviePrincipals', 'Fact_MovieData.movieId = Bridge_MoviePrincipals.movieId'),
    'profession': ('DimProfession', 'Bridge_PrincipalProfessions.professionId = DimProfession.professionId', 'secondary', 'Bridge_PrincipalProfessions', 'Bridge_MoviePrincipals.personId = Bridge_PrincipalProfessions.personId'),

    # Bridge tables
    'Bridge_MovieGenres': ('Bridge_MovieGenres', 'Fact_MovieData.movieId = Bridge_MovieGenres.movieId', 'bridge'),
    'Bridge_MoviePrincipals': ('Bridge_MoviePrincipals', 'Fact_MovieData.movieId = Bridge_MoviePrincipals.movieId', 'bridge'),
    'Bridge_PrincipalProfessions': ('Bridge_PrincipalProfessions', 'Bridge_MoviePrincipals.personId = Bridge_PrincipalProfessions.personId', 'bridge'),
}

def execute_sql_query(query):
    # Connect to the database
    with pyodbc.connect(CONN_STRING) as conn:
        # Execute the query and fetch the results
        df = pd.read_sql(query, conn)

    return df