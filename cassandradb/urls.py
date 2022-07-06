from django.urls import path
from .views import CassandraDatabase

cassandradb = CassandraDatabase()

urlpatterns = [
    path('health-check/', cassandradb.health_check),
    path('create_schema/', cassandradb.create_schema),
    path('drop_schema/', cassandradb.drop_schema),
    path('create_table/', cassandradb.create_table),
    path('drop_table/', cassandradb.drop_table),
    path('insert_record/', cassandradb.insert_record),
    path('insert_multiple_records/', cassandradb.insert_multiple_records),
    path('update_records/', cassandradb.update_records),
    path('retrieve_records/', cassandradb.retrieve_records),
    path('retrieve_records_with_filter/', cassandradb.retrieve_records_with_filter),
    path('delete_records_with_filter/', cassandradb.delete_records_with_filter),
    path('delete_records/', cassandradb.delete_records),
    path('truncate_table/', cassandradb.truncate_table),
]