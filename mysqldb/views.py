from django.shortcuts import render
from django.http import HttpResponse, JsonResponse
from .database import Database
import mysql.connector as conn
from django.views.decorators.csrf import csrf_exempt
import json, csv


class MysqlDatabase(Database):
    def health_check(self, request):
        return JsonResponse("I am up and doing fine", safe=False)

    @csrf_exempt
    def create_schema(self, request):
        try:
            host = request.POST.get('host')
            username = request.POST.get('username')
            password = request.POST.get('password')
            schema_name = request.POST.get('schema_name')
            mysql_db = conn.connect(host=host, user=username, passwd=password, use_pure=True)
            query = f"CREATE DATABASE {schema_name};"
            cursor = mysql_db.cursor()
            cursor.execute(query)
            mysql_db.close()
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
            mysql_db = conn.connect(host=host, user=username, passwd=password, use_pure=True)
            query = f"DROP DATABASE {schema_name};"
            cursor = mysql_db.cursor()
            cursor.execute(query)
            mysql_db.close()
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
            mysql_db = conn.connect(host=host, user=username, passwd=password, use_pure=True, database=schema_name)
            query = f"CREATE TABLE IF NOT EXISTS {table_name}({','.join([x + ' ' + y for x, y in columns.items()])});"
            cursor = mysql_db.cursor()
            cursor.execute(query)
            mysql_db.close()
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
            mysql_db = conn.connect(host=host, user=username, passwd=password, use_pure=True, database=schema_name)
            query = f"DROP TABLE {table_name};"
            cursor = mysql_db.cursor()
            cursor.execute(query)
            mysql_db.close()
            return JsonResponse(f"Table has been dropped dropped from database",safe=False)
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
            values = ','.join('"{0}"'.format(v) for v in columns.values())
            mysql_db = conn.connect(host=host, user=username, passwd=password, use_pure=True, database=schema_name)
            query = f"INSERT INTO {table_name}({','.join(columns.keys())}) VALUES({values});"
            cursor = mysql_db.cursor()
            cursor.execute(query)
            mysql_db.commit()
            mysql_db.close()
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
            print("before connection")
            mysql_db = conn.connect(host=host, user=username, passwd=password, use_pure=True, database=schema_name)
            with open(input_file_name, encoding="utf-8-sig") as csv_f:
                csv_r = csv.reader(csv_f)
                for row in csv_r:
                    values = ','.join('"{0}"'.format(v) for v in row)
                    query = f"INSERT INTO {table_name}({columns_headings}) VALUES({values});"
                    print(query)
                    cursor = mysql_db.cursor()
                    cursor.execute(query)
            mysql_db.commit()
            mysql_db.close()
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
            mysql_db = conn.connect(host=host, user=username, passwd=password, use_pure=True, database=schema_name)
            query = f"UPDATE {table_name} SET {values} WHERE {where};"
            print(query)
            cursor = mysql_db.cursor()
            cursor.execute(query)
            mysql_db.commit()
            mysql_db.close()
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
            mysql_db = conn.connect(host=host, user=username, passwd=password, use_pure=True, database=schema_name)
            query = f"SELECT * FROM {table_name};"
            cursor = mysql_db.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            for r in rows:
                one_row["id"] = r[0]
                one_row["name"] = r[1]
                one_row["age"] = r[2]
                all_rows.append(one_row.copy())
            print("here", rows)
            mysql_db.close()
            return JsonResponse(all_rows,safe=False)
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
            mysql_db = conn.connect(host=host, user=username, passwd=password, use_pure=True, database=schema_name)
            query = f"SELECT * FROM {table_name} WHERE {where};"
            cursor = mysql_db.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            for r in rows:
                one_row["id"] = r[0]
                one_row["name"] = r[1]
                one_row["age"] = r[2]
                all_rows.append(one_row.copy())
            mysql_db.close()
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
            mysql_db = conn.connect(host=host, user=username, passwd=password, use_pure=True, database=schema_name)
            query = f"DELETE FROM {table_name} WHERE {where};"
            cursor = mysql_db.cursor()
            cursor.execute(query)
            mysql_db.commit()
            mysql_db.close()
            return JsonResponse(f"Records deleted from table {table_name.title()}",safe=False)
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
            mysql_db = conn.connect(host=host, user=username, passwd=password, use_pure=True, database=schema_name)
            query = f"DELETE FROM {table_name};"
            cursor = mysql_db.cursor()
            cursor.execute(query)
            mysql_db.commit()
            mysql_db.close()
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
            mysql_db = conn.connect(host=host, user=username, passwd=password, use_pure=True, database=schema_name)
            query = f"TRUNCATE TABLE {table_name};"
            cursor = mysql_db.cursor()
            cursor.execute(query)
            mysql_db.close()
            return JsonResponse(f"Table {table_name.title()} was truncated", safe=False)
        except Exception as exc:
            return JsonResponse(str(exc), safe=False)
