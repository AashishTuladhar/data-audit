import logging
import pathlib
import services
from datetime import datetime


# Class for representing a field description
class Field:
    def __init__(self, field_name, field_index, field_type):
        self.field_name = field_name  # Name of the field
        self.field_index = field_index  # Index of the field
        self.field_type = field_type  # Type of the field


# Collection of Field objects divided into respective domains
class Fields:
    def __init__(self):
        self.primary_key_field = None  # Primary key field
        self.not_null_fields = []  # List of fields that cannot be null
        self.all = []  # List of all fields

    # Set the primary key field
    def set_primary_key_field(self, field_name, field_index, field_type):
        self.primary_key_field = Field(field_name, field_index, field_type)

    # Add a not null field
    def add_not_null_field(self, field_name, field_index, field_type):
        self.not_null_fields.append(Field(field_name, field_index, field_type))

    # Set a field
    def set_field(self, field_name, field_index, field_type):
        self.all.append(Field(field_name, field_index, field_type))

    # Get all fields
    def get_all_fields(self):
        return self.all


# Basic settings object for the validation process
class Validation:
    def __init__(self, validation_file_path, validation_file_name, table_name, separator):
        self.fields = Fields()  # Fields object for storing field information
        self.validation_file_path = validation_file_path  # Path to the validation file
        self.validation_file_name = validation_file_name  # Name of the validation file
        self.table_name = table_name  # Name of the table being validated
        self.separator = separator  # Separator used in the validation file


# Creates and returns the validator class
def validator(validator_path, file_name):
    # Read from the validation file to get all the validation properties
    configurations = services.read_validation_file(validator_path, file_name)
    table_name = configurations[0][1:-2].rstrip()  # Extract table name
    separator = configurations[1].split('=')[1].rstrip()  # Extract separator

    # Create the validation object based on the properties
    validation_obj = Validation(validator_path, file_name, table_name, separator)

    # Search for each record starting with 'Field' in the text file
    for field in filter(lambda x: str(x).startswith('Field') is True, configurations):
        field_items = field.split('=')

        index = int(field_items[0][5:].lstrip().rstrip()) - 1
        name = str(field_items[1].split(',')[0].lstrip().rstrip())
        data_type = str(field_items[1].split(',')[1].lstrip().rstrip())

        validation_obj.fields.set_field(name, index, data_type)

    # Extract and assign primary key and not null fields
    for validation in filter(lambda x: str(x).startswith('/--') is True, configurations):
        validation_items = validation.split('-keyprop')

        field = list(filter(lambda x: x.field_name.lower() == validation_items[1].lstrip().rstrip().lower(),
                            validation_obj.fields.get_all_fields()))[0]

        if validation_items[0].lstrip().rstrip() == '/--PrimaryKeyField':
            validation_obj.fields.set_primary_key_field(field.field_name, field.field_index, field.field_type)
        elif validation_items[0].lstrip().rstrip() == '/--NotNullField':
            validation_obj.fields.add_not_null_field(field.field_name, field.field_index, field.field_type)

    return validation_obj


# Configure the logger
def configure_logger():
    logging.basicConfig(filename=r'{0}\\logs\\audit_report_{1}'.format(str(pathlib.Path().resolve()),
                                                                       datetime.now().strftime('%H_%M_%S.txt')),
                        filemode='w',
                        datefmt='%H:%M:%S',
                        level=logging.INFO)


# Setup function for initializing the validation process
def setup():
    filename = (input("Enter validation file name: ")  # Prompt user for validation file name
                or 'V_Students.txt')
    path = (input("Enter file path: ")  # Prompt user for file path
            or str(pathlib.Path().resolve()) + r'\Validation Files')

    return validator(path, filename)  # Return the validator object
