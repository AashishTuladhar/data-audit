import logging
import pathlib
import services
from datetime import datetime

# class for the field description 
class Field:
    def __init__(self, field_name, field_index, field_type):
        self.field_name = field_name
        self.field_index = field_index
        self.field_type = field_type

# collection of field class divided into respective domains
class Fields:
    def __init__(self):
        self.primary_key_field = None
        self.not_null_fields = []
        self.all = []

    def set_primary_key_field(self, field_name, field_index, field_type):
        self.primary_key_field = Field(field_name, field_index, field_type)

    def add_not_null_field(self, field_name, field_index, field_type):
        self.not_null_fields.append(Field(field_name, field_index, field_type))

    def set_field(self, field_name, field_index, field_type):
        self.all.append(Field(field_name, field_index, field_type))

    def get_all_fields(self):
        return self.all

# basic settings object for the validation process
class Validation:
    def __init__(self, validation_file_path, validation_file_name, table_name, separator, date_format):
        self.fields = Fields()
        self.validation_file_path = validation_file_path
        self.validation_file_name = validation_file_name
        self.table_name = table_name
        self.separator = separator
        self.date_format = date_format

# creates and returns the validator class
def validator(validator_path, file_name):
    # read from the valdiation file to get all the validation properties
    configurations = services.read_validation_file(validator_path, file_name)
    table_name = configurations[0][1:-2].rstrip()
    separator = configurations[1].split('=')[1].rstrip()
    date_format = configurations[2].split('=')[1].rstrip()

    # create the validation object based on the properties
    validation_obj = Validation(validator_path, file_name, table_name, separator, date_format)

    # search for each record starting with 'Field' in the text file
    for field in filter(lambda x: str(x).startswith('Field') is True, configurations):
        field_items = field.split('=')

        index = int(field_items[0][5:].lstrip().rstrip()) - 1
        name = str(field_items[1].split(',')[0].lstrip().rstrip())
        data_type = str(field_items[1].split(',')[1].lstrip().rstrip())

        validation_obj.fields.set_field(name, index, data_type)

    for validation in filter(lambda x: str(x).startswith('/--') is True, configurations):
        validation_items = validation.split('-keyprop')

        field = list(filter(lambda x: x.field_name.lower() == validation_items[1].lstrip().rstrip().lower(),
                            validation_obj.fields.get_all_fields()))[0]

        if validation_items[0].lstrip().rstrip() == '/--PrimaryKeyField':
            validation_obj.fields.set_primary_key_field(field.field_name, field.field_index, field.field_type)
        elif validation_items[0].lstrip().rstrip() == '/--NotNullField':
            validation_obj.fields.add_not_null_field(field.field_name, field.field_index, field.field_type)

    return validation_obj


def configure_logger():
    logging.basicConfig(filename=str(pathlib.Path().resolve()) + r'\logs\audit_report_' + datetime.now().strftime('%H_%M_%S.txt'),
                        filemode='w',
                        # format='%(pastime)s,%(secs)d %(name)s %(levelness)s %(message)s',
                        datefmt='%H:%M:%S',
                        level=logging.INFO)


def setup():
    filename = (input("Enter validation file name: ")
                or 'V_Products.txt')
    path = (input("Enter file path: ")
            or str(pathlib.Path().resolve()) + r'\Validation Files')
    configure_logger()

    return validator(path, filename)
