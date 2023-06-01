from flask import Flask, render_template
from flask_restful import Resource, Api, reqparse
import pyodbc
import pandas as pd
from io import StringIO
from datetime import datetime

app = Flask(__name__)
api = Api(app)

base_query = '''WITH base_query AS (
	SELECT	
		emp.id AS empId, 
		emp.ename AS name, 
		emp.datetime AS datetime, 
		jobs.id AS jobId, 
		jobs.job AS job, 
		dep.id AS depId, 
		dep.department AS department 
	FROM dbo.hired_employees emp
	inner join dbo.jobs jobs
		ON emp.job_id = jobs.id
	inner join dbo.departments dep
		ON emp.department_id = dep.id
	WHERE year(emp.datetime) = 2021
),

ALLQ AS(
	SELECT	
		job, 
		department,	
		DATEPART(QUARTER, datetime) as Q, 
		COUNT(*) AS amount 
	FROM base_query
	GROUP BY job, department, DATEPART(QUARTER, datetime)
),
'''

quarters_query = base_query + \
'''
quarters AS(
	SELECT 
		department, 
		job, 
		COALESCE([1], 0) Q1,
		COALESCE([2], 0) Q2,
		COALESCE([3], 0) Q3,
		COALESCE([4], 0) Q4 
	FROM ALLQ
	PIVOT(MAX(amount)
	FOR Q IN ([1],[2],[3],[4])) q
)

SELECT * FROM quarters
ORDER BY department, job;'''

median_query = base_query + \
'''MEDIAN AS(
	SELECT AVG(TOTAL) MEDIA 
	FROM(
		SELECT SUM(AMOUNT) TOTAL from ALLQ
		GROUP BY department
	) FINAL
),

TOTAL_DEP AS(
	select 
		depId AS id, 
		department, 
		COUNT(*) hired from 
	base_query
	GROUP BY depId, department
),

MAY_DEP AS(
	SELECT TOT_DEP.* FROM (
		SELECT * FROM TOTAL_DEP
	)TOT_DEP,
	(
		SELECT * FROM MEDIAN
	)MED
	WHERE TOT_DEP.hired > MED.MEDIA
)

SELECT * FROM MAY_DEP
ORDER BY hired DESC;'''

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

@app.route('/totalDep')
def totalDep():
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()

    df = pd.read_sql(quarters_query, cnxn)

    cnxn.commit()
    cursor.close()

    return render_template('index.html',  tables=[df.to_html(classes='data')], titles=df.columns.values)

@app.route('/mostDep')
def mostDep():
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()

    df = pd.read_sql(median_query, cnxn)

    cnxn.commit()
    cursor.close()

    return render_template('index.html',  tables=[df.to_html(classes='data')], titles=df.columns.values)


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


