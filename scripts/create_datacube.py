import sys
import argparse
import warnings
from utils import dim_table_map, execute_sql_query


# Filter out UserWarning category warnings
warnings.filterwarnings('ignore', category=UserWarning)


def build_dynamic_query(selected_measures, selected_dims):
    select_clause = []
    join_clauses = []
    group_by_clause = []
    order_by_clause = []

    # Add measures to the select clause
    for measure in selected_measures:
        agg_function = 'AVG' if measure == 'averageRating' else 'SUM'
        select_clause.append(f'{agg_function}(Fact_MovieData.{measure}) AS {measure}')
        order_by_clause.append(f'{agg_function}(Fact_MovieData.{measure}) DESC')

    # Process dimensions and determine join conditions
    for dim in selected_dims:
        if dim in dim_table_map:
            table, join_condition, dim_type, *bridge_info = dim_table_map[dim]
            select_clause.append(f'{table}.{dim}')
            group_by_clause.append(f'{table}.{dim}')

            if dim_type == 'secondary' and bridge_info:
                bridge_table, bridge_condition = bridge_info[:2]
                join_clauses.append(f'JOIN {bridge_table} ON {bridge_condition}')
                join_clauses.append(f'JOIN {table} ON {join_condition}')
            elif dim_type == 'primary':
                join_clauses.append(f'JOIN {table} ON {join_condition}')

    # Special handling for the profession dimension
    if 'profession' in selected_dims:
        join_clauses.clear()  # Clear existing joins to handle profession specifically
        join_clauses.append('JOIN Bridge_MoviePrincipals ON Fact_MovieData.movieId = Bridge_MoviePrincipals.movieId')
        join_clauses.append('JOIN Bridge_PrincipalProfessions ON Bridge_MoviePrincipals.personId = Bridge_PrincipalProfessions.personId')
        join_clauses.append('JOIN DimProfession ON Bridge_PrincipalProfessions.professionId = DimProfession.professionId')

    # Combine all joins
    join_clause_string = ' '.join(join_clauses)

    # Build the complete SQL query
    query = f"""
    SELECT {', '.join(select_clause)}
    FROM Fact_MovieData
    {join_clause_string}
    GROUP BY {', '.join(group_by_clause)}
    ORDER BY {', '.join(order_by_clause)}
    """
    return query


def main():
    # Create the parser
    parser = argparse.ArgumentParser(description='Create data cube from movie database.')

    # Add arguments
    parser.add_argument('--measure', choices=['averageRating', 'numVotes', 'both'], default='both',
                        help='Choose the measure(s) for the data cube. Options: averageRating, numVotes, both.')
    user_friendly_dims = [dim for dim, details in dim_table_map.items() if details[-1] != 'bridge']
    parser.add_argument('--dim', required=True, choices=user_friendly_dims,
                    help='Choose one dimension from the list of available dimensions.')
    parser.add_argument('--output', default='../analysis_results/output.csv',
                        help='Specify the output file path for the data cube. Default is "../analysis_results/output.csv".')

    # Check if no arguments were provided (just the script name)
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    
    # Parse arguments
    args = parser.parse_args()

    # Handle measures
    if args.measure == 'both':
        selected_measures = ['averageRating', 'numVotes']
    else:
        selected_measures = [args.measure]

    # Handle dimensions
    selected_dims = [args.dim]

    # Build and execute query
    query = build_dynamic_query(selected_measures, selected_dims)
    result_df = execute_sql_query(query)

    # Save to CSV
    result_df.to_csv(args.output, index=False)
    print(f'Data cube saved to {args.output}')


if __name__ == "__main__":
    main()
