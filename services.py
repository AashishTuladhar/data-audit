def read_data_file(path, filename):
    with open(f'{path}\\{filename}', 'r') as file:
        for line in file:
            yield line.rstrip()


def read_validation_file(path, filename):
    with open(f'{path}\\{filename}', 'r') as file:
        return file.readlines()
