from prefectstone.tasks.gcp import BigQueryConsultTask
import os

def dowload_and_save(query, file_format='csv'):
    bqct = BigQueryConsultTask(env='prd')
    df = bqct.run(filename=query)
    filename = fr"data\{query[:-4]}.{file_format}"
    df.to_csv(filename, index=False)
    print(f"'{filename}' saved")


dowload_and_save('raiox_consulta_atualizado.sql', file_format='csv')