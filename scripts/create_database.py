import os
import pyodbc
import pandas as pd
from utils import db_config, CONN_STRING


def create_database(db_config):
    conn_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={db_config["server"]},{db_config["port"]};UID={db_config["username"]};PWD={db_config["password"]}'

    # Connect to the server without specifying a database
    conn = pyodbc.connect(conn_string, autocommit=True)
    cursor = conn.cursor()

    # SQL query to create the new database
    create_db_query = f"IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = N'{db_config['database']}') BEGIN CREATE DATABASE [{db_config['database']}] END"

    # Execute the query to create the database
    try:
        cursor.execute(create_db_query)
        print(f"Database '{db_config['database']}' created successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        cursor.close()
        conn.close()


def drop_database(db_config):
    conn_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={db_config["server"]},{db_config["port"]};UID={db_config["username"]};PWD={db_config["password"]}'

    # Connect to the server without autocommit
    conn = pyodbc.connect(conn_string)
    # Enable autocommit
    conn.autocommit = True
    cursor = conn.cursor()

    # SQL queries to set the database to single user mode and drop it
    set_single_user_query = f"ALTER DATABASE [{db_config['database']}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE"
    drop_db_query = f"DROP DATABASE [{db_config['database']}]"

    # Execute the queries to set to single user mode and drop the database
    try:
        cursor.execute(set_single_user_query)
        cursor.execute(drop_db_query)
        print(f"Database '{db_config['database']}' dropped successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        cursor.close()
        conn.close()


def create_and_import_dim_movie(csv_path):    
    # SQL query to create the DimMovie table with an additional column for runtimeMinutes
    create_table_query = """
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[DimMovie]') AND type in (N'U'))
    CREATE TABLE DimMovie (
        movieId VARCHAR(255) PRIMARY KEY,
        titleType VARCHAR(255),
        primaryTitle VARCHAR(255),
        originalTitle VARCHAR(255),
        isAdult BIT,
        startYear INT,
        endYear INT NULL,
        runtimeMinutes INT NULL
    )
    """

    # Connect to the database
    with pyodbc.connect(CONN_STRING) as conn:
        cursor = conn.cursor()
        # Create DimMovie table
        cursor.execute(create_table_query)
        print("DimMovie table created successfully.")

        # Import data from CSV to DimMovie table
        movies_df = pd.read_csv(csv_path)

        # Convert 'isAdult' to boolean (True/False)
        movies_df['isAdult'] = movies_df['isAdult'].apply(lambda x: True if x == 'True' else False)

        # Convert 'startYear', 'endYear', and 'runtimeMinutes' to integers or None
        movies_df['startYear'] = movies_df['startYear'].apply(lambda x: int(x) if pd.notnull(x) else None)
        movies_df['endYear'] = movies_df['endYear'].apply(lambda x: int(x) if pd.notnull(x) else None)
        movies_df['runtimeMinutes'] = movies_df['runtimeMinutes'].apply(lambda x: int(x) if pd.notnull(x) else None)

        # Replace NaN with None for SQL compatibility
        movies_df = movies_df.where(pd.notnull(movies_df), None)
        
        for index, row in movies_df.iterrows():
            cursor.execute("INSERT INTO DimMovie (movieId, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", 
                           row['movieId'], row['titleType'], row['primaryTitle'], row['originalTitle'], row['isAdult'], row['startYear'], row['endYear'], row['runtimeMinutes'])
        print("Data imported into DimMovie table successfully.")


def create_and_import_dim_genre(csv_path):
    # SQL query to create the DimGenre table
    create_table_query = """
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[DimGenre]') AND type in (N'U'))
    CREATE TABLE DimGenre (
        genreId INT PRIMARY KEY,
        genreName VARCHAR(255)
    )
    """

    # Connect to the database
    with pyodbc.connect(CONN_STRING) as conn:
        cursor = conn.cursor()
        # Create DimGenre table
        cursor.execute(create_table_query)
        print("DimGenre table created successfully.")

        # Import data from CSV to DimGenre table
        genres_df = pd.read_csv(csv_path)
        for index, row in genres_df.iterrows():
            cursor.execute("INSERT INTO DimGenre (genreId, genreName) VALUES (?, ?)", 
                           row['genreId'], row['genreName'])
        print("Data imported into DimGenre table successfully.")


def create_and_import_dim_date(csv_path):
    # SQL query to create the DimDate table
    create_table_query = """
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[DimDate]') AND type in (N'U'))
    CREATE TABLE DimDate (
        year INT,
        dateKey INT PRIMARY KEY
    )
    """

    # Connect to the database
    with pyodbc.connect(CONN_STRING) as conn:
        cursor = conn.cursor()

        # Create DimDate table
        cursor.execute(create_table_query)
        print("DimDate table created successfully.")

        # Import data from CSV to DimDate table
        dates_df = pd.read_csv(csv_path)
        for index, row in dates_df.iterrows():
            # Convert numpy.int64 to native Python int
            year = int(row['year']) if pd.notnull(row['year']) else None
            dateKey = int(row['dateKey']) if pd.notnull(row['dateKey']) else None
            cursor.execute("INSERT INTO DimDate (year, dateKey) VALUES (?, ?)", year, dateKey)
        print("Data imported into DimDate table successfully.")


def create_and_import_dim_person(csv_path):
    # SQL query to create the DimPerson table
    create_table_query = """
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[DimPerson]') AND type in (N'U'))
    CREATE TABLE DimPerson (
        personId VARCHAR(255) PRIMARY KEY,
        name VARCHAR(255),
        birthYear INT NULL,
        deathYear INT NULL
    )
    """

    # Connect to the database
    with pyodbc.connect(CONN_STRING) as conn:
        cursor = conn.cursor()

        # Create DimPerson table
        cursor.execute(create_table_query)
        print("DimPerson table created successfully.")

        # Import data from CSV to DimPerson table
        persons_df = pd.read_csv(csv_path)
        # Handle NULL values for birthYear and deathYear
        for index, row in persons_df.iterrows():
            birthYear = int(row['birthYear']) if pd.notnull(row['birthYear']) else None
            deathYear = int(row['deathYear']) if pd.notnull(row['deathYear']) else None

            cursor.execute("INSERT INTO DimPerson (personId, name, birthYear, deathYear) VALUES (?, ?, ?, ?)", 
                           row['personId'], row['name'], birthYear, deathYear)
        print("Data imported into DimPerson table successfully.")


def create_and_import_dim_profession(csv_path):
    # SQL query to create the DimProfession table
    create_table_query = """
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[DimProfession]') AND type in (N'U'))
    CREATE TABLE DimProfession (
        professionId INT PRIMARY KEY,
        profession VARCHAR(255)
    )
    """

    # Connect to the database
    with pyodbc.connect(CONN_STRING) as conn:
        cursor = conn.cursor()

        # Create DimProfession table
        cursor.execute(create_table_query)
        print("DimProfession table created successfully.")

        # Import data from CSV to DimProfession table
        professions_df = pd.read_csv(csv_path)
        for index, row in professions_df.iterrows():
            cursor.execute("INSERT INTO DimProfession (professionId, profession) VALUES (?, ?)", 
                           row['professionId'], row['profession'])
        print("Data imported into DimProfession table successfully.")



def create_and_import_bridge_movie_genres(csv_path):
    # SQL query to create the Bridge_MovieGenres table
    create_table_query = """
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Bridge_MovieGenres]') AND type in (N'U'))
    CREATE TABLE Bridge_MovieGenres (
        movieId VARCHAR(255),
        genreId INT,
        PRIMARY KEY (movieId, genreId),
        FOREIGN KEY (movieId) REFERENCES DimMovie(movieId),
        FOREIGN KEY (genreId) REFERENCES DimGenre(genreId)
    )
    """

    # Connect to the database
    with pyodbc.connect(CONN_STRING) as conn:
        cursor = conn.cursor()
        
        # Create Bridge_MovieGenres table
        cursor.execute(create_table_query)
        print("Bridge_MovieGenres table created successfully.")

        # Import data from CSV to Bridge_MovieGenres table
        bridge_df = pd.read_csv(csv_path)
        for index, row in bridge_df.iterrows():
            cursor.execute("INSERT INTO Bridge_MovieGenres (movieId, genreId) VALUES (?, ?)", 
                           row['movieId'], row['genreId'])
        print("Data imported into Bridge_MovieGenres table successfully.")


def create_and_import_bridge_movie_principals(csv_path):
    # SQL query to create the Bridge_MoviePrincipals table
    create_table_query = """
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Bridge_MoviePrincipals]') AND type in (N'U'))
    CREATE TABLE Bridge_MoviePrincipals (
        principalId INT PRIMARY KEY,
        movieId VARCHAR(255),
        ordering INT,
        personId VARCHAR(255),
        job VARCHAR(255),
        characters VARCHAR(255),
        professionId INT,
        FOREIGN KEY (movieId) REFERENCES DimMovie(movieId),
        FOREIGN KEY (personId) REFERENCES DimPerson(personId),
        FOREIGN KEY (professionId) REFERENCES DimProfession(professionId)
    )
    """

    # Connect to the database
    with pyodbc.connect(CONN_STRING) as conn:
        cursor = conn.cursor()

        # Create Bridge_MoviePrincipals table
        cursor.execute(create_table_query)
        print("Bridge_MoviePrincipals table created successfully.")

        # Import data from CSV to Bridge_MoviePrincipals table
        bridge_df = pd.read_csv(csv_path)
        for index, row in bridge_df.iterrows():
            # Convert to int, handling NULL values
            ordering = int(row['ordering']) if pd.notnull(row['ordering']) else None
            professionId = int(row['professionId']) if pd.notnull(row['professionId']) else None

            # Convert to string, handling NULL values
            job = str(row['job']) if pd.notnull(row['job']) else None
            characters = str(row['characters']) if pd.notnull(row['characters']) else None
            cursor.execute("INSERT INTO Bridge_MoviePrincipals (principalId, movieId, ordering, personId, job, characters, professionId) VALUES (?, ?, ?, ?, ?, ?, ?)", 
                           row['principalId'], row['movieId'], ordering, row['personId'], job, characters, professionId)
        print("Data imported into Bridge_MoviePrincipals table successfully.")


def create_and_import_bridge_principal_professions(csv_path):
    # SQL query to create the Bridge_PrincipalProfessions table
    create_table_query = """
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Bridge_PrincipalProfessions]') AND type in (N'U'))
    CREATE TABLE Bridge_PrincipalProfessions (
        personId VARCHAR(255),
        professionId INT,
        PRIMARY KEY (personId, professionId),
        FOREIGN KEY (personId) REFERENCES DimPerson(personId),
        FOREIGN KEY (professionId) REFERENCES DimProfession(professionId)
    )
    """

    # Connect to the database
    with pyodbc.connect(CONN_STRING) as conn:
        cursor = conn.cursor()

        # Create Bridge_PrincipalProfessions table
        cursor.execute(create_table_query)
        print("Bridge_PrincipalProfessions table created successfully.")

        # Import data from CSV to Bridge_PrincipalProfessions table
        bridge_df = pd.read_csv(csv_path)
        for index, row in bridge_df.iterrows():
            cursor.execute("INSERT INTO Bridge_PrincipalProfessions (personId, professionId) VALUES (?, ?)", 
                           row['personId'], row['professionId'])
        print("Data imported into Bridge_PrincipalProfessions table successfully.")


def create_and_import_fact_movie_data(csv_path):
    # SQL query to create the Fact_MovieData table
    create_table_query = """
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Fact_MovieData]') AND type in (N'U'))
    CREATE TABLE Fact_MovieData (
        factId INT PRIMARY KEY,
        movieId VARCHAR(255),
        dateKey INT,
        averageRating FLOAT,
        numVotes INT,
        FOREIGN KEY (movieId) REFERENCES DimMovie(movieId),
        FOREIGN KEY (dateKey) REFERENCES DimDate(dateKey)
    )
    """

    # Connect to the database
    with pyodbc.connect(CONN_STRING) as conn:
        cursor = conn.cursor()

        # Create Fact_MovieData table
        cursor.execute(create_table_query)
        print("Fact_MovieData table created successfully.")

        # Import data from CSV to Fact_MovieData table
        facts_df = pd.read_csv(csv_path)
        for index, row in facts_df.iterrows():
            cursor.execute("INSERT INTO Fact_MovieData (factId, movieId, dateKey, averageRating, numVotes) VALUES (?, ?, ?, ?, ?)", 
                           row['factId'], row['movieId'], row['dateKey'], row['averageRating'], row['numVotes'])
        print("Data imported into Fact_MovieData table successfully.")


if __name__ == "__main__":
    # csv data folder
    data_folder = '../datasets_star/'

    create_database(db_config)
    create_and_import_dim_movie(data_folder + 'DimMovie.csv')
    create_and_import_dim_genre(data_folder + 'DimGenre.csv')
    create_and_import_dim_date(data_folder + 'DimDate.csv')
    create_and_import_dim_person(data_folder + 'DimPerson.csv')
    create_and_import_dim_profession(data_folder + 'DimProfession.csv')
    create_and_import_bridge_movie_genres(data_folder + 'Bridge_MovieGenres.csv')
    create_and_import_bridge_movie_principals(data_folder + 'Bridge_MoviePrincipals.csv')
    create_and_import_bridge_principal_professions(data_folder + 'Bridge_PrincipalProfessions.csv')
    create_and_import_fact_movie_data(data_folder + 'Fact_MovieData.csv')

    # drop_database(db_config)