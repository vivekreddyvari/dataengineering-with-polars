from dataclasses import dataclass
from typing import List
import polars as pl
import datetime as dt
from polars.dataframe.frame import DataFrame
from abc import ABC, abstractmethod
import logging as LOG


# Logging
@dataclass
class Logger:
    logger_name: str = 'my_logger'

    def __post_init__(self):
        """Set up the logger with basic configuration upon instantiation."""
        self.logger = self.log_basic_config()

    def log_basic_config(self) -> LOG.Logger:
        """Set basic configuration for logging and return the logger instance."""
        LOG.basicConfig(level=LOG.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        logger = LOG.getLogger(self.logger_name)
        return logger

    def get_logger(self) -> LOG.Logger:
        """Return the configured logger instance."""
        return self.logger


# Data Transfer Object (DTO)
@dataclass
class PersonalData:
    name: str
    birthdate: dt.date
    weight: float
    height: float
    bmi: float


# Interface for data operations
class DataFrameOperations(ABC):
    @abstractmethod
    def create_dataframe(self, data: List[PersonalData]) -> DataFrame:
        pass

    @abstractmethod
    def validate_data(self, data: List[PersonalData]) -> bool:
        pass


# Data Operations for Dataframe
class PolarsDFOperation(DataFrameOperations):

    def __init__(self, logger: LOG.Logger):
        self.log = logger

    def validate_data(self, data: List[PersonalData]) -> bool:
        """validate input data """

        # Validate the datatype
        try:
            for person in data:
                if not all(
                    [
                        isinstance(person.name, str),
                        isinstance(person.birthdate, dt.date),
                        isinstance(person.weight, (int, float)),
                        isinstance(person.height, float),
                        isinstance(person.bmi, float)
                    ]
                ):
                    return False
            return True
        except Exception as e:
            self.log.error(f"Validation Error : {e}")
            return False

    def create_dataframe(self, data: List[PersonalData]) -> DataFrame:
        """ Create a polar dataframe """
        try:
            if not self.validate_data(data):
                raise ValueError("Invalid Data Format")
            df_dict = {
                "name": [person.name for person in data],
                "birthdate": [person.birthdate for person in data],
                "weight": [person.weight for person in data],
                "height": [person.height for person in data],
                "bmi": [person.bmi for person in data]
            }
            return pl.DataFrame(df_dict)
        except Exception as e:
            self.log.error(f"Error in creating DataFrame : {e}")
            raise


# Implementation Class
class PolarsTutorial:

    def __init__(self, data_operation: DataFrameOperations, log: Logger):
        self.data_operation = data_operation
        self.log = log.get_logger()

    def process_data(self, data: List[PersonalData]) -> DataFrame:
        return self.data_operation.create_dataframe(data)


# Factory class for creating sample data:
class DataFactory:
    @staticmethod
    def create_data() -> List[PersonalData]:
        return [
            PersonalData("VR", dt.date(1986, 1, 1), 75.0, 166.0, 0.0),
            PersonalData("RP", dt.date(1991, 1, 1), 65.5, 166.0, 0.0),
            PersonalData("KR", dt.date(2019, 1, 1), 15.5, 110.0, 0.0),
            PersonalData("SSR", dt.date(2022, 1, 1), 13.5, 100.0, 0.0),
        ]


# main
def main() -> DataFrame:
    log = Logger()
    logger = log.get_logger()

    try:
        # Create sample data
        data = DataFactory.create_data()

        # Create concrete implementation of DataFrameOperations
        df_operations = PolarsDFOperation(logger)

        # Create PolarsTutorial instance with concrete implementation
        polars_tutorial = PolarsTutorial(data_operation=df_operations, log=log)

        # create dataframe
        df = polars_tutorial.process_data(data)
        logger.info(f"Dataframe successfully created")

    except Exception as e:
        logger.error(f"Error in main: {e}")
        return None

    return df


# if __name__ == "__main__":
    # main()
