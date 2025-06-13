import pandas as pd


class DataAnalyzer:
    """
    Clase for connecting to a database and analyzing null values in a DataFrame.
    """

    def __init__(self, db_connection):
        """
        Initializes the class with a database connection object.

        Args:
            db_connection: Object that provides methods to connect to the database and execute queries.
        """
        self.db = db_connection

    def fetch_data(self, query):
        """
        Connect to the database and execute a query to fetch data as a DataFrame.

        Args:
            query (str): SQL query to execute.

        Returns:
            pd.DataFrame or None: DataFrame  with the results of the query or None if an error occurs.
        """
        try:
            self.db.connect()
            results = self.db.fetch_all(query)
            columns = [desc[0] for desc in self.db.cursor.description]
            return pd.DataFrame(results, columns=columns)
        except Exception as error:
            print(f"Error fetching data: {error}")
            return None

    @staticmethod
    def analyze_null_values(dataframe):
        """
        Analyze and display statistics of null values in a DataFrame.

        Args:
            dataframe (pd.DataFrame): DataFrame to analyze.

        Returns:
            None
        """
        null_counts = dataframe.isnull().sum()
        null_percentage = (null_counts / len(dataframe)) * 100

        total_cells = dataframe.size
        total_nulls = null_counts.sum()
        global_null_percentage = (total_nulls / total_cells) * 100

        print("Null counts per column (descending order):")
        print(null_counts.sort_values(ascending=False))

        print("\nNull percentage per column (descending order):")
        print(null_percentage.sort_values(ascending=False).round(2))

        print("\nGlobal null value analysis:")
        print(f"- Total cells: {total_cells:,}")
        print(f"- Total null values: {total_nulls:,}")
        print(f"- Global null percentage: {global_null_percentage:.2f}%")

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the DataFrame:
         - Fill null values in station columns with 'Unknown'
         - Remove rows without destination coordinates

        Args:
            df (pd.DataFrame): DataFrame

        Returns:
            pd.DataFrame: cleaned DataFrame
        """
        columnas_estacion = [
            "start_station_name", "start_station_id",
            "end_station_name", "end_station_id"
        ]
        for col in columnas_estacion:
            df[col] = df[col].fillna("Unknown")

        df = df.dropna(subset=["end_lat", "end_lng"])
        return df
    
    # Functio to add new columns to the DataFrame
    def add_time_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """        
        Add new columns to the DataFrame based on the 'started_at' column:
            - 'hour': Hour of the trip  
            - 'day_of_week': Day of the week of the trip
            - 'month': Month of the trip
            - 'year': Year of the trip
        Args:
            df (pd.DataFrame): DataFrame with a 'started_at' column of datetime type.
        Returns:
            pd.DataFrame: DataFrame with new columns added.
        """
        # get the trop duration in minutes
        df['trip_duration'] = (df['ended_at'] - df['started_at']).dt.total_seconds() / 60  # Trip duration in minutes
        # Get the day of the week for each trip
        df['day_of_week'] = df['started_at'].dt.day_name()
        # Get the month name for each trip
        df['month'] = df['started_at'].dt.month
        # get the year for each trip
        df['year'] = df['started_at'].dt.year
        
        return df