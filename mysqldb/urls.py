from django.urls import path
from .views import MysqlDatabase

mysqldb = MysqlDatabase()

urlpatterns = [
    path('health-check/', mysqldb.health_check),
    path('create_schema/', mysqldb.create_schema),
    path('drop_schema/', mysqldb.drop_schema),
    path('create_table/', mysqldb.create_table),
    path('drop_table/', mysqldb.drop_table),
    path('insert_record/', mysqldb.insert_record),
    path('insert_multiple_records/', mysqldb.insert_multiple_records),
    path('update_records/', mysqldb.update_records),
    path('retrieve_records/', mysqldb.retrieve_records),
    path('retrieve_records_with_filter/', mysqldb.retrieve_records_with_filter),
    path('delete_records_with_filter/', mysqldb.delete_records_with_filter),
    path('delete_records/', mysqldb.delete_records),
    path('truncate_table/', mysqldb.truncate_table),
]