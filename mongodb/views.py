# Create your views here.
from django.http import HttpResponse, JsonResponse
from .database import Database
from django.views.decorators.csrf import csrf_exempt
import pymongo, csv, json
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


class MongoDatabase(Database):
    def health_check(self, request):
        return JsonResponse("I am up and doing fine", safe=False)

    @csrf_exempt
    def create_schema(self, request):
        try:
            host = request.POST.get('host')
            username = request.POST.get('username')
            password = request.POST.get('password')
            schema_name = request.POST.get('schema_name')
            client = pymongo.MongoClient("mongodb+srv://vamsi:vamsi@vamsi.fknwz.mongodb.net/?retryWrites=true&w=majority")
            db = client[schema_name]
            collection = db["db_log"]
            record = {"event": "database created"}
            collection.insert_one(record)
            return JsonResponse("Database has been created successfully", safe=False)
        except Exception as exc:
            return JsonResponse(str(exc), safe=False)

    @csrf_exempt
    def drop_schema(self, request):
        try:
            host = request.POST.get('host')
            username = request.POST.get('username')
            password = request.POST.get('password')
            schema_name = request.POST.get('schema_name')
            client = pymongo.MongoClient("mongodb+srv://vamsi:vamsi@vamsi.fknwz.mongodb.net/?retryWrites=true&w"
                                         "=majority")
            client.drop_database(schema_name)
            return JsonResponse("Database has been dropped successfully", safe=False)
        except Exception as exc:
            return JsonResponse(str(exc), safe=False)

    @csrf_exempt
    def create_table(self, request):
        try:
            host = request.POST.get('host')
            username = request.POST.get('username')
            password = request.POST.get('password')
            schema_name = request.POST.get('schema_name')
            table_name = request.POST.get('table_name')
            columns = json.loads(request.POST.get('columns'))
            client = pymongo.MongoClient("mongodb+srv://vamsi:vamsi@vamsi.fknwz.mongodb.net/?retryWrites=true&w"
                                         "=majority")
            db = client[schema_name]
            collection = db[table_name]
            columns["__table structure__"] = "This is to show the column constraints"
            collection.insert_one(columns)
            return JsonResponse("Table has been successfully", safe=False)
        except Exception as exc:
            return JsonResponse(str(exc), safe=False)

    @csrf_exempt
    def drop_table(self, request):
        try:
            host = request.POST.get('host')
            username = request.POST.get('username')
            password = request.POST.get('password')
            schema_name = request.POST.get('schema_name')
            table_name = request.POST.get('table_name')
            client = pymongo.MongoClient("mongodb+srv://vamsi:vamsi@vamsi.fknwz.mongodb.net/?retryWrites=true&w=majority")
            db = client[schema_name]
            collection = db[table_name]
            collection.drop()
            return JsonResponse(f"Table has been dropped dropped from database", safe=False)
        except Exception as exc:
            return JsonResponse(str(exc), safe=False)

    @csrf_exempt
    def insert_record(self, request):
        try:
            host = request.POST.get('host')
            username = request.POST.get('username')
            password = request.POST.get('password')
            schema_name = request.POST.get('schema_name')
            table_name = request.POST.get('table_name')
            columns = json.loads(request.POST.get('columns'))
            client = pymongo.MongoClient("mongodb+srv://vamsi:vamsi@vamsi.fknwz.mongodb.net/?retryWrites=true&w=majority")
            db = client[schema_name]
            collection = db[table_name]
            collection.insert_one(columns)
            return JsonResponse(f"Row inserted into table {table_name.title()}", safe=False)
        except Exception as exc:
            return JsonResponse(str(exc), safe=False)

    @csrf_exempt
    def insert_multiple_records(self, request):
        try:
            host = request.POST.get('host')
            username = request.POST.get('username')
            password = request.POST.get('password')
            schema_name = request.POST.get('schema_name')
            table_name = request.POST.get('table_name')
            input_file_name = request.POST.get('input_file_name')
            columns = json.loads(request.POST.get('columns').replace("'", '"'))
            client = pymongo.MongoClient("mongodb+srv://vamsi:vamsi@vamsi.fknwz.mongodb.net/?retryWrites=true&w=majority")
            db = client[schema_name]
            collection = db[table_name]
            with open(input_file_name, encoding="utf-8-sig") as csv_f:
                csv_r = csv.reader(csv_f)
                for row in csv_r:
                    key_values = {columns[i]: row[i] for i in range(len(columns))}
                    collection.insert_one(key_values)
            return JsonResponse(f"Rows inserted into table {table_name.title()}", safe=False)
        except Exception as exc:
            return JsonResponse(str(exc), safe=False)

    @csrf_exempt
    def update_records(self, request):
        try:
            host = request.POST.get('host')
            username = request.POST.get('username')
            password = request.POST.get('password')
            schema_name = request.POST.get('schema_name')
            table_name = request.POST.get('table_name')
            filters = json.loads(request.POST.get('filters'))
            updated_values = json.loads(request.POST.get('updated_values'))
            client = pymongo.MongoClient("mongodb+srv://vamsi:vamsi@vamsi.fknwz.mongodb.net/?retryWrites=true&w=majority")
            db = client[schema_name]
            collection = db[table_name]
            new_values = {"$set": updated_values}
            print("before update", new_values)
            collection.update_many(filters, new_values)
            return JsonResponse(f"Row updated in table {table_name.title()}", safe=False)
        except Exception as exc:
            return JsonResponse(str(exc), safe=False)

    @csrf_exempt
    def retrieve_records(self, request):
        try:
            one_row = {}
            all_rows = []
            host = request.POST.get('host')
            username = request.POST.get('username')
            password = request.POST.get('password')
            schema_name = request.POST.get('schema_name')
            table_name = request.POST.get('table_name')
            result = []
            client = pymongo.MongoClient("mongodb+srv://vamsi:vamsi@vamsi.fknwz.mongodb.net/?retryWrites=true&w=majority")
            db = client[schema_name]
            collection = db[table_name]
            rows = collection.find({}, {"_id": 0, "id": 1, "name": 1, "age": 1})
            for r in rows:
                result.append(r)
            return JsonResponse(result, safe=False)
        except Exception as exc:
            return JsonResponse(str(exc), safe=False)

    @csrf_exempt
    def retrieve_records_with_filter(self, request):
        one_row = {}
        all_rows = []
        try:
            host = request.POST.get('host')
            username = request.POST.get('username')
            password = request.POST.get('password')
            schema_name = request.POST.get('schema_name')
            table_name = request.POST.get('table_name')
            filters = json.loads(request.POST.get('filters'))
            result = []
            client = pymongo.MongoClient("mongodb+srv://vamsi:vamsi@vamsi.fknwz.mongodb.net/?retryWrites=true&w=majority")
            db = client[schema_name]
            collection = db[table_name]
            rows = collection.find(filters, {"_id": 0, "id": 1, "name": 1, "age": 1})
            for r in rows:
                result.append(r)
            return JsonResponse(result, safe=False)
        except Exception as exc:
            return JsonResponse(str(exc), safe=False)

    @csrf_exempt
    def delete_records_with_filter(self, request):
        try:
            host = request.POST.get('host')
            username = request.POST.get('username')
            password = request.POST.get('password')
            schema_name = request.POST.get('schema_name')
            table_name = request.POST.get('table_name')
            filters = json.loads(request.POST.get('filters'))
            where = 'and '.join(f"{k} = '{v}'" for k, v in filters.items())
            client = pymongo.MongoClient("mongodb+srv://vamsi:vamsi@vamsi.fknwz.mongodb.net/?retryWrites=true&w=majority")
            db = client[schema_name]
            collection = db[table_name]
            collection.delete_many(filters)
            return JsonResponse(f"Records deleted from table {table_name.title()}", safe=False)
        except Exception as exc:
            return JsonResponse(str(exc), safe=False)

    @csrf_exempt
    def delete_records(self, request):
        try:
            host = request.POST.get('host')
            username = request.POST.get('username')
            password = request.POST.get('password')
            schema_name = request.POST.get('schema_name')
            table_name = request.POST.get('table_name')
            client = pymongo.MongoClient("mongodb+srv://vamsi:vamsi@vamsi.fknwz.mongodb.net/?retryWrites=true&w=majority")
            db = client[schema_name]
            collection = db[table_name]
            collection.delete_many({})
            return JsonResponse(f"All records deleted from table {table_name.title()}", safe=False)
        except Exception as exc:
            return JsonResponse(str(exc), safe=False)

    @csrf_exempt
    def truncate_table(self, request):
        try:
            host = request.POST.get('host')
            username = request.POST.get('username')
            password = request.POST.get('password')
            schema_name = request.POST.get('schema_name')
            table_name = request.POST.get('table_name')
            client = pymongo.MongoClient("mongodb+srv://vamsi:vamsi@vamsi.fknwz.mongodb.net/?retryWrites=true&w=majority")
            db = client[schema_name]
            collection = db[table_name]
            collection.remove({})
            return JsonResponse(f"Table {table_name.title()} was truncated", safe=False)
        except Exception as exc:
            return JsonResponse(str(exc), safe=False)

