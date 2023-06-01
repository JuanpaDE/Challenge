import pandas as pd
import requests
import json

url = 'http://127.0.0.1:5000/csv_migration'


filesSchema = {'hired_employees':{'id':'int64',
                                  'eName':'object',
                                  'datetime':'object',
                                  'department_id':'float64',
                                  'job_id':'float64'},
                'departments':{'id':'int64',
                               'department':'object'},
                'jobs':{'id':'int64',
                        'job':'object'}}

path = 'C:/Users/RentAdvisor/Documents/Personal/Globant/data_challenge_files/'

for file, schema in filesSchema.items():

    df = pd.read_csv(f'{path}{file}.csv', header=None)
    df.columns = list(schema.keys())

    packQty = len(df)//1000

    if len(df) % 1000 > 0:
        packQty += 1

    for pack in range(packQty):
        initPos = pack*1000

        finPos = initPos + len(df) % 1000 if pack == packQty else pack*1000 + 1000

        df_to_Send = df.iloc[initPos:finPos,:]
        csvFile = df_to_Send.to_csv(index=False)

        datos = {"fileName": file,
                 "fileData": csvFile}

        respuesta = requests.post(url, data=json.dumps(datos), headers = {"Content-Type": "application/json"})

        if respuesta.status_code == 200:
            print('Datos enviados correctamente.')
        else:
            print('Error al enviar los datos. Código de estado:', respuesta.status_code)