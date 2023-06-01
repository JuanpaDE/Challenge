import pandas as pd
import requests
import json

url = 'http://127.0.0.1:5000/csv_migration'


filesSchema = {'departments':{'id':'int64',
                            'department':'object'},
                'jobs':{'id':'int64',
                        'job':'object'},
                'hired_employees':{'id':'int64',
                                  'eName':'object',
                                  'datetime':'object',
                                  'department_id':'float64',
                                  'job_id':'float64'},
                }

path = './data_challenge_files/'

for file, schema in filesSchema.items():

    df = pd.read_csv(f'{path}{file}.csv', header=None)
    df.columns = list(schema.keys())

    # Amount of packages
    packQty = len(df)//1000

    if len(df) % 1000 > 0:
        packQty += 1
    
    # Sending packages
    for pack in range(packQty):
        initPos = pack*1000

        # In the last packet only the remaining data should be sent
        finPos = initPos + len(df) % 1000 if pack == packQty else pack*1000 + 1000

        df_to_Send = df.iloc[initPos:finPos,:]
        csvFile = df_to_Send.to_csv(index=False)

        datos = {"fileName": file,
                 "fileData": csvFile}

        # Request
        respuesta = requests.post(url, data=json.dumps(datos), headers = {"Content-Type": "application/json"})

        if respuesta.status_code == 200:
            print('Datos enviados correctamente.')
        else:
            print('Error al enviar los datos. Código de estado:', respuesta.status_code)