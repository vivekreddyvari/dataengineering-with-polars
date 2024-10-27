import os
from polars.dataframe.frame import DataFrame
from enjoy_first_program import main, Logger
import polars as pl
import datetime as dt
from dataclasses import dataclass, field


@dataclass
class CSVFunctions:
    log: Logger = field(default_factory=Logger)
    df: DataFrame = None

    def __post_init__(self):
        """Set up the DataFrame when the class is instantiated."""
        self.df = main()  # Ensure main() returns a DataFrame

    def write_to_csv(self, filename: str) -> None:
        """Writes the DataFrame to a CSV file."""
        if not isinstance(filename, str):
            self.log.get_logger().error("FileName must be `str`")
            raise ValueError("FileName must be `str`")
        if filename.strip():
            # Ensure the directory exists
            directory = os.path.dirname(f"../{filename}.csv")
            if not os.path.exists(directory):
                os.makedirs(directory)  # create a directory

        if filename.strip():
            # Add two columns birth year, and calculate bmi
            self.df = self.df.with_columns(
                birth_year=pl.col("birthdate").dt.year(),
                bmi=pl.col("weight") / (pl.col("height") ** 2),
            )
            self.df.write_csv(f"../{filename}.csv")
            self.log.get_logger().info(f"Successfully written to CSV")
        else:
            self.log.get_logger().error("FileName must NOT be NULL")
            raise ValueError("FileName must NOT be NULL")

    def read_the_csv(self, filename: str) -> DataFrame:
        """Reads a CSV file into a DataFrame."""
        if not isinstance(filename, str):
            self.log.get_logger().error("FileName must be `str`")
            raise ValueError("FileName must be `str`")

        if filename.strip():
            df_csv = pl.read_csv(f"../{filename}.csv", try_parse_dates=True)
            self.log.get_logger().info(f"Reading the CSV file `{filename}`")

        else:
            self.log.get_logger().error("FileName must NOT be NULL")
            raise ValueError("FileName must NOT be NULL")
        return df_csv


if __name__ == "__main__":
    # instantiate CSVFunctions
    csv_functions = CSVFunctions()

    # write
    file_name = "data/family_bmi"
    csv_functions.write_to_csv(filename=file_name)

    # read
    df_csv = csv_functions.read_the_csv(filename=file_name)
    print(f" Dataframe: Complete DF: {df_csv}")

    # filter
    result = df_csv.filter(pl.col("birthdate").dt.year() < 1990)
    print(f"Function: FILTER: {result}")

    # filter with between
    result_btw = df_csv.filter(
        pl.col("birthdate").is_between(dt.date(1982, 12, 31), dt.date(2019, 1, 1)),
        pl.col("height") > 100,
    )
    print(f"Function: IN BETWEEN: {result_btw}")

    # # only for display
    # result_df = df_csv.select(
    #    pl.col("name"),
    #    pl.col("birthdate").dt.year().alias("birth_year"),
    #    (pl.col("weight") / (pl.col("height") ** 2)).alias("bmi"),
    #)
    # print(result_df)
