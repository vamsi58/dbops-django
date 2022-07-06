from abc import ABC, abstractmethod


class Database(ABC):
    @abstractmethod
    def create_schema(self, request):
        pass

    @abstractmethod
    def drop_schema(self, request):
        pass

    @abstractmethod
    def create_table(self, request):
        pass

    @abstractmethod
    def drop_table(self, request):
        pass

    @abstractmethod
    def insert_record(self, request):
        pass

    @abstractmethod
    def insert_multiple_records(self, request):
        pass

    @abstractmethod
    def update_records(self, request):
        pass

    @abstractmethod
    def retrieve_records(self, request):
        pass

    @abstractmethod
    def retrieve_records_with_filter(self, request):
        pass

    @abstractmethod
    def delete_records_with_filter(self, request):
        pass

    @abstractmethod
    def delete_records(self, request):
        pass

    @abstractmethod
    def truncate_table(self, request):
        pass
