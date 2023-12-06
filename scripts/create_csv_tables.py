import os
import pandas as pd


def create_dim_movie(movies_csv_path, save_path):
    # Load the movies data
    movies_df = pd.read_csv(movies_csv_path)
    
    # Create the DimMovie DataFrame by selecting relevant columns
    dim_movie = movies_df[['tconst', 'titleType', 'primaryTitle', 'originalTitle', 'isAdult', 'startYear', 'endYear', 'runtimeMinutes']].copy()
    dim_movie.rename(columns={'tconst': 'movieId'}, inplace=True)

    # Convert isAdult to a boolean type
    dim_movie['isAdult'] = dim_movie['isAdult'].astype(bool)

    # Replace '\\N' with the string 'NULL'
    dim_movie['startYear'].replace('\\N', 'NULL', inplace=True)
    dim_movie['endYear'].replace('\\N', 'NULL', inplace=True)
    
    # Save the DimMovie DataFrame to a CSV file
    dim_movie.to_csv(os.path.join(save_path, 'DimMovie.csv'), index=False)
    
    return dim_movie


# Function to create DimGenre
def create_dim_genre(movies_csv_path, save_path):
    # Load the genres data
    genres_df = pd.read_csv(movies_csv_path)
    
    # Save the DimGenre DataFrame to a CSV file
    genres_df.to_csv(save_path + 'DimGenre.csv', index=False)
    
    return genres_df


# Function to create Bridge_MovieGenres
def create_bridge_movie_genres(movies_csv_path, save_path):
    # Load the movie genres data
    movie_genres_df = pd.read_csv(movies_csv_path)
    
    # Rename the columns to match the star schema
    movie_genres_df.rename(columns={'tconst': 'movieId', 'genreId': 'genreId'}, inplace=True)
    
    # Save the Bridge_MovieGenres DataFrame to a CSV file
    movie_genres_df.to_csv(save_path + 'Bridge_MovieGenres.csv', index=False)
    
    return movie_genres_df


# Function to create DimPerson
def create_dim_person(names_csv_path, save_path):
    # Load the names data
    names_df = pd.read_csv(names_csv_path)
    
    # Rename the columns to match the star schema
    names_df.rename(columns={'nconst': 'personId', 'primaryName': 'name', 
                             'birthYear': 'birthYear', 'deathYear': 'deathYear'}, inplace=True)
    
    # Replace '\\N' with the string 'NULL'
    names_df['birthYear'].replace('\\N', 'NULL', inplace=True)
    names_df['deathYear'].replace('\\N', 'NULL', inplace=True)
    
    # Save the DimPerson DataFrame to a CSV file
    names_df.to_csv(os.path.join(save_path, 'DimPerson.csv'), index=False)
    
    return names_df


def create_dim_profession(professions_csv_path, save_path):
    # Load the professions data
    professions_df = pd.read_csv(professions_csv_path)
    
    # Rename the columns to match the star schema
    professions_df.rename(columns={'professionId': 'professionId', 'profession': 'profession'}, inplace=True)
    
    # Save the DimProfession DataFrame to a CSV file
    professions_df.to_csv(os.path.join(save_path, 'DimProfession.csv'), index=False)
    
    return professions_df


def create_bridge_movie_principals(principals_csv_path, save_path):
    # Load the principals data
    principals_df = pd.read_csv(principals_csv_path)
    
    # Rename the columns to match the star schema and align with the Dimension tables
    principals_df.rename(columns={'tconst': 'movieId', 'nconst': 'personId', 
                                  'professionId': 'professionId', 'principalId': 'principalId'}, inplace=True)

    # Replace '\\N' with the string 'NULL' for columns that need it
    principals_df['job'].replace('\\N', 'NULL', inplace=True)
    principals_df['characters'].replace('\\N', 'NULL', inplace=True)

    # Save the Bridge_MoviePrincipals DataFrame to a CSV file
    principals_df.to_csv(os.path.join(save_path, 'Bridge_MoviePrincipals.csv'), index=False)
    
    return principals_df


def create_bridge_principal_professions(name_professions_csv_path, save_path):
    # Load the name-professions relationship data
    name_professions_df = pd.read_csv(name_professions_csv_path)
    
    # Rename the columns to match the star schema and align with the Dimension tables
    name_professions_df.rename(columns={'nconst': 'personId', 'professionId': 'professionId'}, inplace=True)
    
    # Save the Bridge_PrincipalProfessions DataFrame to a CSV file
    name_professions_df.to_csv(os.path.join(save_path, 'Bridge_PrincipalProfessions.csv'), index=False)
    
    return name_professions_df


def create_dim_date(movies_csv_path, save_path, year_extension=5):
    # Load the movies data
    movies_df = pd.read_csv(movies_csv_path)

    # Find the minimum and maximum years from the startYear column
    # Assuming missing or malformed years are handled or filtered out
    min_year = movies_df['startYear'].min()
    max_year = movies_df['startYear'].max()

    # Extend the year range by a specified amount
    start_year = min_year - year_extension
    end_year = max_year + year_extension

    # Creating a DataFrame for years
    years = range(start_year, end_year + 1)
    dim_date = pd.DataFrame(years, columns=['year'])
    dim_date['dateKey'] = dim_date['year']
    
    # Save the DimDate DataFrame to a CSV file
    dim_date.to_csv(os.path.join(save_path, 'DimDate.csv'), index=False)
    
    return dim_date


def create_fact_movie_data(movies_csv_path, save_path):
    # Load the movies data
    movies_df = pd.read_csv(movies_csv_path)

    # Create the Fact_MovieData DataFrame
    fact_movie_data = movies_df[['tconst', 'startYear', 'averageRating', 'numVotes']].copy()
    fact_movie_data.rename(columns={'tconst': 'movieId', 'startYear': 'dateKey'}, inplace=True)
    
    # Optionally, generate a unique ID for each fact record
    fact_movie_data['factId'] = range(1, len(fact_movie_data) + 1)
    
    # Save the Fact_MovieData DataFrame to a CSV file
    fact_movie_data.to_csv(os.path.join(save_path, 'Fact_MovieData.csv'), index=False)
    
    return fact_movie_data


if __name__ == "__main__":
    # csv orig_folder and saved_folder
    orig_folder = '../../../IMDB_top250/datasets_top250/'
    saved_folder = '../datasets_star/'

    create_dim_movie(orig_folder + 'movies.csv', saved_folder)
    create_dim_genre(orig_folder + 'genres.csv', saved_folder)
    create_bridge_movie_genres(orig_folder + 'movie_genres.csv', saved_folder)
    create_dim_person(orig_folder + 'names.csv', saved_folder)
    create_dim_profession(orig_folder + 'professions.csv', saved_folder)
    create_bridge_movie_principals(orig_folder + 'principals.csv', saved_folder)
    create_bridge_principal_professions(orig_folder + 'name_professions.csv', saved_folder)
    create_dim_date(orig_folder + 'movies.csv', saved_folder)
    create_fact_movie_data(orig_folder + 'movies.csv', saved_folder)