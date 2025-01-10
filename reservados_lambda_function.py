#lambda para ETL do reservados

import boto3
import mysql.connector
import csv
import os
from datetime import datetime
from io import StringIO

def get_secret():
    secret_name = "banco-centralizado"
    region_name = "us-east-1"  # Altere para a região correta, como "us-east-1"

    client = boto3.client("secretsmanager", region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    
    return eval(response['SecretString'])

def lambda_handler(event, context):
    try:
        # Recuperar credenciais do Secrets Manager
        secrets = get_secret()
        
        # Conectar ao banco de dados MySQL
        conn = mysql.connector.connect(
            host=secrets["host"],
            user=secrets["username"],
            password=secrets["password"],
            database=secrets["dbname"]
        )
        cursor = conn.cursor()

        # Executar a consulta desejada
        query = """
        SELECT 
            CONCAT(RS.cn,RS.prefixo,RS.mcdu) AS "tn",
            RS.cn,
            RS.prefixo,
            RS.mcdu,
            RS.rn1_cadup,
            RS.rn1_portado,
            RS.contrato,
            RS.canais,
            RS.ilimitado
        FROM reservados as RS
        ORDER BY CONCAT(RS.cn,RS.prefixo,RS.mcdu)
        limit 10
        """
        cursor.execute(query)
        
        # Buscar todos os registros
        records = cursor.fetchall()

        # Definir o caminho do arquivo CSV com a data no formato mmddaa
        #today = datetime.today()
        #file_name = today.strftime("%m%d%y") + "-resultado.csv"

        # Usar StringIO para escrever o CSV em memória
        #csv_buffer = StringIO()
        #csv_writer = csv.writer(csv_buffer)
        #csv_writer.writerow([i[0] for i in cursor.description])  # Cabeçalho
        #csv_writer.writerows(records)

        # Enviar o arquivo CSV para o S3
        #s3 = boto3.client('s3')
        #bucket_name = "nome-do-bucket"  # Substitua pelo nome do seu bucket
        #s3.put_object(Bucket=bucket_name, Key=file_name, Body=csv_buffer.getvalue())

        #cursor.close()
        #conn.close()

        return {
            'statusCode': 200,
            'body': 'Arquivo CSV gerado e salvo no S3 com sucesso.'
        }

    except mysql.connector.Error as err:
        return {
            'statusCode': 500,
            'body': f"Erro no MySQL: {err}"
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f"Erro: {str(e)}"
        }