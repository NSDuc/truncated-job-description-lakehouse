import os


class MedallionTableDTO:
    def __init__(self, base_path, file_format='delta'):
        self._base_path = base_path
        self._file_format = file_format

    def base_path(self):
        return self._base_path

    def file_format(self):
        return self._file_format

    def data_dirpath(self):
        return os.path.join(self._base_path, 'data')

    def checkpoint_dirpath(self):
        return os.path.join(self._base_path, 'checkpoint')
