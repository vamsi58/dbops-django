from django.urls import path
from .views import MongoDatabase

mongodb = MongoDatabase()

urlpatterns = [
    path('health-check/', mongodb.health_check),
    path('create_schema/', mongodb.create_schema),
    path('drop_schema/', mongodb.drop_schema),
    path('create_table/', mongodb.create_table),
    path('drop_table/', mongodb.drop_table),
    path('insert_record/', mongodb.insert_record),
    path('insert_multiple_records/', mongodb.insert_multiple_records),
    path('update_records/', mongodb.update_records),
    path('retrieve_records/', mongodb.retrieve_records),
    path('retrieve_records_with_filter/', mongodb.retrieve_records_with_filter),
    path('delete_records_with_filter/', mongodb.delete_records_with_filter),
    path('delete_records/', mongodb.delete_records),
    path('truncate_table/', mongodb.truncate_table),
]