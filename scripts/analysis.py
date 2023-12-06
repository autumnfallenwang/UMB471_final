import os
import sys
import argparse
import pandas as pd
import matplotlib.pyplot as plt
from create_datacube import build_dynamic_query
from utils import execute_sql_query


analysis_tasks = [
    {
        "index": 1,
        "problem_description": "Analyze the average ratings of movies across different genres.",
        "auto_generate": True,
        "SQL_query_params": {
            "selected_measures": ["averageRating"],
            "selected_dims": ["genreName"]
        },
        "SQL_query": "auto",
        "output": {
            "data_file": "1.csv",
            "figure_file": "1.png"
        },
        "visualization_details": {
            "chart_type": "bar",
            "axes_info": {
                "x_axis": "genreName",
                "y_axis": "averageRating"
            },
            "title": "Average Movie Ratings by Genre",
            "x_label": "Genre",
            "y_label": "Average Rating"
        }
    },
    {
        "index": 2,
        "problem_description": "Analyze how movie ratings have changed over the year. Group movies by the year of their release and compare their average ratings.",
        "auto_generate": True,
        "SQL_query_params": {
            "selected_measures": ["averageRating"],
            "selected_dims": ["startYear"]
        },
        "SQL_query": "auto",
        "output": {
            "data_file": "2.csv",
            "figure_file": "2.png"
        },
        "visualization_details": {
            "chart_type": "scatter",
            "axes_info": {
                "x_axis": "startYear",
                "y_axis": "averageRating",
            },
            "title": "Average Movie Ratings by Year",
            "x_label": "Year",
            "y_label": "Average Rating"
        }
    },
    {
        "index": 3,
        "problem_description": "Analyze the relationship between movie duration (runtime) and average ratings, and see how this relationship varies across different genres. The goal is to find out if longer or shorter movies tend to receive higher ratings and if this trend is consistent across genres.",
        "auto_generate": False,
        "SQL_query": """
            SELECT DimGenre.genreName, AVG(Fact_MovieData.averageRating) AS averageRating, AVG(DimMovie.runtimeMinutes) AS averageRuntime
            FROM Fact_MovieData
            JOIN DimMovie ON Fact_MovieData.movieId = DimMovie.movieId
            JOIN Bridge_MovieGenres ON DimMovie.movieId = Bridge_MovieGenres.movieId
            JOIN DimGenre ON Bridge_MovieGenres.genreId = DimGenre.genreId
            GROUP BY DimGenre.genreName
        """,
        "output": {
            "data_file": "3.csv",
            "figure_file": "3.png"
        },
        "visualization_details": {
            "chart_type": "scatter",
            "axes_info": {
                "x_axis": "averageRuntime",
                "y_axis": "averageRating",
            },
            "title": "Average Ratings vs. Runtime by Genre",
            "x_label": "Average Runtime (minutes)",
            "y_label": "Average Rating",
            "annotate": "genreName"
        }
    },
    {
        "index": 4,
        "problem_description": "Correlation between the birth year of movie personnel and movie average ratings",
        "auto_generate": False,
        "SQL_query": """
            SELECT DimPerson.birthYear, AVG(Fact_MovieData.averageRating) AS averageRating
            FROM Fact_MovieData
            JOIN Bridge_MoviePrincipals ON Fact_MovieData.movieId = Bridge_MoviePrincipals.movieId
            JOIN DimPerson ON Bridge_MoviePrincipals.personId = DimPerson.personId
            WHERE DimPerson.birthYear IS NOT NULL
            GROUP BY DimPerson.birthYear
            ORDER BY DimPerson.birthYear
        """,
        "output": {
            "data_file": "4.csv",
            "figure_file": "4.png"
        },
        "visualization_details": {
            "chart_type": "scatter",
            "axes_info": {
                "x_axis": "birthYear",
                "y_axis": "averageRating"
            },
            "title": "Average Movie Ratings by Birth Year of Personnel",
            "x_label": "Birth Year",
            "y_label": "Average Rating"
        }
    },
    {
        "index": 5,
        "problem_description": "Trend in movie runtime over years and its relation to average ratings",
        "auto_generate": False,
        "SQL_query": """
            SELECT DimMovie.startYear, AVG(Fact_MovieData.averageRating) AS averageRating, AVG(DimMovie.runtimeMinutes) AS averageRuntime
            FROM Fact_MovieData
            JOIN DimMovie ON Fact_MovieData.movieId = DimMovie.movieId
            WHERE DimMovie.startYear IS NOT NULL AND DimMovie.runtimeMinutes IS NOT NULL
            GROUP BY DimMovie.startYear
            ORDER BY DimMovie.startYear
        """,
        "output": {
            "data_file": "5.csv",
            "figure_file": "5.png"
        },
        "visualization_details": {
            "chart_type": "scatter",
            "axes_info": {
                "x_axis": "startYear",
                "y_axis": "averageRuntime",
            },
            "title": "Movie Runtime and Ratings Over Years",
            "x_label": "Start Year",
            "y_label": "Average Runtime (minutes)",
            "annotate": "averageRating"
        }
    }
]


def run_analysis_engine(task, output_folder):
    # Ensure output directory exists
    os.makedirs(output_folder, exist_ok=True)

    print(f"Processing task {task['index']}: {task['problem_description']}")

    # Build and execute the SQL query
    if task['auto_generate']:
        query = build_dynamic_query(task['SQL_query_params']['selected_measures'],
                                    task['SQL_query_params']['selected_dims'])
    else:
        query = task['SQL_query']

    df = execute_sql_query(query)

    # Save the result data to a CSV file
    csv_file_path = os.path.join(output_folder, task['output']['data_file'])
    df.to_csv(csv_file_path, index=False)
    print(f"Data saved to {csv_file_path}")

    # Visualization
    if 'figure_file' in task['output']:
        visualize_data(df, task['visualization_details'])
        fig_file_path = os.path.join(output_folder, task['output']['figure_file'])
        plt.savefig(fig_file_path)
        print(f"Figure saved to {fig_file_path}")


def visualize_data(df, vis_details):
    # Create plot
    df.plot(kind=vis_details['chart_type'],
            x=vis_details['axes_info']['x_axis'],
            y=vis_details['axes_info']['y_axis'],
            title=vis_details['title'],
            xlabel=vis_details['x_label'],
            ylabel=vis_details['y_label'],
            grid=True,
            figsize=(10, 6))

    # Add annotations if required
    annotation_column = vis_details.get("annotate")
    if annotation_column:
        for i, point in df.iterrows():
            # Format the annotation string for floating-point values
            annotation_text = "{:.2f}".format(point[annotation_column]) if isinstance(point[annotation_column], float) else str(point[annotation_column])
            plt.text(point[vis_details['axes_info']['x_axis']], 
                     point[vis_details['axes_info']['y_axis']], 
                     annotation_text)


def show_analysis_tasks(analysis_tasks, verbose=False):
    for task in analysis_tasks:
        print(f"Task {task['index']}: {task['problem_description']}")
        if verbose:
            print(f"Auto Generate Query: {task['auto_generate']}")
            if task['auto_generate']:
                print(f"SQL Query Params: {task['SQL_query_params']}")
            else:
                print(f"SQL Query: {task['SQL_query']}")
            print(f"Output Files: Data - {task['output']['data_file']}, Figure - {task['output']['figure_file']}")
            print(f"Visualization Details: {task['visualization_details']}")
        print()


def main():
    parser = argparse.ArgumentParser(description="Data Analysis Tool for Movie Database")
    parser.add_argument('--show', action='store_true', help='Show the list of analysis tasks.')
    parser.add_argument('--verbose', action='store_true', help='Show detailed information for tasks.')
    parser.add_argument('--task', nargs='*', default='all', help='Specify the analysis tasks to run by index or "all" for all tasks.')
    parser.add_argument('--run', action='store_true', help='Run the specified analysis tasks.')
    parser.add_argument('--output', default='../analysis_results', help='Specify the output folder for analysis results. Default is "../analysis_results"')

    # Check if no arguments were provided (just the script name)
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()

    if args.show:
        show_analysis_tasks(analysis_tasks, verbose=args.verbose)
    
    if args.run:
        task_indices = args.task
        available_indices = [task['index'] for task in analysis_tasks]

        if 'all' in task_indices or not task_indices:
            task_indices = available_indices
        else:
            # Convert to integers and filter available indices
            task_indices = [int(index) for index in task_indices]

            # Warn about unavailable task indices
            for index in task_indices:
                if index not in available_indices:
                    print(f"Error: Task {index} does not exist in the task list.")

            # Keep only available task indices
            task_indices = [index for index in task_indices if index in available_indices]

        for index in task_indices:
            task = next((task for task in analysis_tasks if task['index'] == index), None)
            if task:
                run_analysis_engine(task, output_folder=args.output)
            else:
                print(f"Warning: Task {index} does not exist in the task list.")


if __name__ == "__main__":
    main()

