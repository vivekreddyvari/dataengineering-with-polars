import os
from polars.dataframe.frame import DataFrame
from enjoy_first_program import main, Logger
import polars as pl
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
                os.makedirs(directory) # create a directory

        if filename.strip():
            self.df.write_csv(f"../{filename}.csv")
            self.log.get_logger().info(f"Successfully written to CSV")
        else:
            self.log.get_logger().error("FileName must NOT be NULL")
            raise ValueError("FileName must NOT be NULL")

    def read_the_csv(self, filename: str ) -> None:
        """Reads a CSV file into a DataFrame."""
        if not isinstance(filename, str):
            self.log.get_logger().error("FileName must be `str`")
            raise ValueError("FileName must be `str`")

        if filename.strip():
            df_csv = pl.read_csv(f"../{filename}.csv", try_parse_dates=True)
            self.log.get_logger().info(f"Reading the CSV file `{filename}`")
            print(df_csv)
        else:
            self.log.get_logger().error("FileName must NOT be NULL")
            raise ValueError("FileName must NOT be NULL")

if __name__ == "__main__":

    # instantiate CSVFunctions
    csv_functions = CSVFunctions()

    # write
    file_name = "data/family_bmi"
    csv_functions.write_to_csv(filename=file_name)

    # read
    csv_functions.read_the_csv(filename=file_name)

