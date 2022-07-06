# Create your views here.
from django.http import HttpResponse, JsonResponse
from .database import Database
from django.views.decorators.csrf import csrf_exempt
import json, csv
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


class CassandraDatabase(Database):
    def health_check(self, request):
        return JsonResponse("I am up and doing fine", safe=False)

    @csrf_exempt
    def create_schema(self, request):
        try:
            host = request.POST.get('host')
            username = request.POST.get('username')
            password = request.POST.get('password')
            schema_name = request.POST.get('schema_name')
            session = self.connect_database()
            replica = " with replication={ 'class': 'SimpleStrategy', 'replication_factor' : 3};"
            query = f"CREATE KEYSPACE IF NOT EXISTS {schema_name} " + replica
            session.execute(query)
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
            session = self.connect_database()
            query = f"DROP KEYSPACE IF EXISTS {schema_name}"
            print(query)
            session.execute(query)
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
            session = self.connect_database()
            query = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name}({','.join([x + ' ' + y for x, y in columns.items()])});"
            print(query)
            session.execute(query)
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
            session = self.connect_database()
            query = f"DROP TABLE IF EXISTS {schema_name}.{table_name};"
            print(query)
            session.execute(query)
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
            values = ','.join("'{0}'".format(v) for v in columns.values())
            session = self.connect_database()
            query = f"INSERT INTO {schema_name}.{table_name}({','.join(columns.keys())}) VALUES({values} ); "
            session.execute(query)
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
            columns_headings = ','.join(columns)
            session = self.connect_database()
            with open(input_file_name, encoding="utf-8-sig") as csv_f:
                csv_r = csv.reader(csv_f)
                for row in csv_r:
                    values = ','.join("'{0}'".format(v) for v in row)
                    query = f"INSERT INTO {schema_name}.{table_name}({columns_headings}) VALUES({values}); "
                    session.execute(query)
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
            values = ','.join(f"{k} = '{v}'" for k, v in updated_values.items())
            where = 'and '.join(f"{k} = '{v}'" for k, v in filters.items())
            session = self.connect_database()
            query = f"UPDATE {schema_name}.{table_name} SET {values} WHERE {where} ALLOW FILTERING;"
            session.execute(query)
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
            session = self.connect_database()
            query = f"SELECT * FROM {schema_name}.{table_name};"
            rows = session.execute(query)
            print("Rows,", rows)
            for r in rows:
                one_row["id"] = r[0]
                one_row["name"] = r[1]
                one_row["age"] = r[2]
                all_rows.append(one_row.copy())
            return JsonResponse(all_rows, safe=False)
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
            where = 'and '.join(f"{k} = '{v}'" for k, v in filters.items())
            session = self.connect_database()
            query = f"SELECT * FROM {schema_name}.{table_name} WHERE {where} ALLOW FILTERING;"
            rows = session.execute(query)
            print("Rows,", rows)
            for r in rows:
                one_row["id"] = r[0]
                one_row["name"] = r[1]
                one_row["age"] = r[2]
                all_rows.append(one_row.copy())
            return JsonResponse(all_rows, safe=False)
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
            session = self.connect_database()
            query = f"DELETE FROM {schema_name}.{table_name} WHERE {where};"
            print(query)
            session.execute(query)
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
            session = self.connect_database()
            query = f"DELETE FROM {schema_name}.{table_name} WHERE id > '2';"
            session.execute(query)
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
            session = self.connect_database()
            query = f"TRUNCATE TABLE {schema_name}.{table_name};"
            session.execute(query)
            return JsonResponse(f"Table {table_name.title()} was truncated", safe=False)
        except Exception as exc:
            return JsonResponse(str(exc), safe=False)

    def connect_database(self):
        cloud_config = {
            'secure_connect_bundle': 'cassandradb/secure-connect-db1.zip'
        }
        auth_provider = PlainTextAuthProvider('JCQsdZKAaPYDalZjBcXbIdQt',
                                              '1bpGqn6G1rot8P+vq7,Y_JyOmd,qnRU8ima-sHlmw--Hip_dMFMiFbzNmWejTh68vFPgs.be2oBKSAdQbNW_ts3KJT6.B20ELtlqmceaHc6eSeyqcwt9BYd.N1Nd3X_9')
        cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        session = cluster.connect()
        return session
