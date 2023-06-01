from flask import Flask, render_template
from flask_restful import Resource, Api, reqparse
import pyodbc
import pandas as pd
from io import StringIO
from datetime import datetime

app = Flask(__name__)
api = Api(app)

server = 'sqlserverchallenge.database.windows.net'
database = 'sqldb_challenge'
username = 'sql_admin'
password = 'jp@927*Wrr'


filesSchema = {'hired_employees':{'id':'int64',
                                  'eName':'object',
                                  'datetime':'object',
                                  'department_id':'float64',
                                  'job_id':'float64'},
                'departments':{'id':'int64',
                               'department':'object'},
                'jobs':{'id':'int64',
                        'job':'object'}}





class csv_migration(Resource):
        
    def post(self):        
        parser = reqparse.RequestParser()
        parser.add_argument('fileName', required=True, type=str)
        parser.add_argument('fileData', required=True, type=str)

        args = parser.parse_args()

        fileName = args['fileName']
        fileData = args['fileData']
        
        try:
            data = StringIO(fileData)
            df=pd.read_csv(data)

            df = df.dropna()
            
            expected_dtypes = list(filesSchema[fileName.lower()].values())
            input_dtypes = [str(list(df.dtypes)[i]) for i in range(len(list(df.dtypes)))]

            expected_columns = list(filesSchema[fileName.lower()].keys())
            input_columns = list(df.columns)

            if (expected_dtypes != input_dtypes) or (expected_columns != input_columns):
                return {'message': "Esquema incompatible con la tabal de destino"}, 400
            
        except:
            return {'message': "Archivo no valido."}, 500

        # Insert Dataframe into SQL Server:
        if len(df) > 1000:
            return {'message':'El archivo supera las 1000 filas.'}, 400

        cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = cnxn.cursor()

        for index, row in df.iterrows():
            try:
                if fileName == 'hired_employees':
                    cursor.execute("INSERT INTO dbo.hired_employees (id,eName,datetime,department_id,job_id) values(?,?,?,?,?)", int(row.id), str(row.eName), datetime.strptime(row.datetime, '%Y-%m-%dT%H:%M:%SZ'), int(row.department_id), int(row.job_id))
                elif fileName == 'departments':
                    cursor.execute("INSERT INTO dbo.departments (id,department) values(?,?)", int(row.id), str(row.department))
                elif fileName == 'jobs':
                    cursor.execute("INSERT INTO dbo.jobs (id,job) values(?,?)", int(row.id), str(row.job))
            except:
                print('Data Error')
        cnxn.commit()
        cursor.close()
        
        return {'message': "Datos CSV procesados correctamete."}, 200

api.add_resource(csv_migration, '/csv_migration')

if __name__ == "__main__":
    app.run(debug=True)


