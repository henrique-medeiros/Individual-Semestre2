from json import loads
from time import sleep
from tokenize import String
from urllib3 import PoolManager
import datetime
from datetime import datetime
import psutil
import mysql.connector
import time 
import os
from mysql.connector import errorcode
import pyodbc
import textwrap3



try:
        # Conexão com o banco de dados (Azure)
        driver= '{ODBC Driver 18 for SQL Server}'
        server_name = 'projeto-rec'
        database_name = 'REC'
        server = '{server_name}.database.windows.net,1433'.format(server_name=server_name)
        username = 'grupo09'
        password = 'M@umau03221'

        connection_string = textwrap3.dedent('''
        Driver={driver};
        Server={server};
        Database={database};
        Uid={username};
        Pwd={password};
        Encrypt=yes;
        TrustedServerCertificate=no;
        Connection Timeout=10;
        '''.format(
        driver=driver,
        server=server,
        database=database_name,
        username=username,
        password=password
        ))
        cnxn:pyodbc.Connection = pyodbc.connect(connection_string)
        print("Conectei no banco! (Azure)")

        # Conexão com o banco de dados (Local)
        db_connection = mysql.connector.connect(
                host='localhost', user='root', password='#Gf47139014825', database='REC')
        print("Conectei no banco! (Local)")
except mysql.connector.Error as error:
        if error.errno == errorcode.ER_BAD_DB_ERROR:
                print("Não encontrei o banco")
        elif error.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Credenciais erradas")
        else:
                print(error)



        #Temperatura
def conversor(valor):
        return float(valor[0:4].replace(",", '.'))
            

def leitura(db_connection):
        cursorLocal = db_connection.cursor()
        cursorAzure = cnxn.cursor()

            # row = cursor.fetchone()
            # Equip = int(''.join(map(str, row)))

with PoolManager() as pool:
    while True:
        response = pool.request('GET', 'http://localhost:9000/data.json')
        data = loads(response.data.decode('utf-8'))
        temp_min = conversor(data['Children'][0]['Children'][1]['Children'][1]['Children'][0]['Min'])
        temp_value = conversor(data['Children'][0]['Children'][1]['Children'][1]['Children'][0]['Value'])
        temp_max = conversor(data['Children'][0]['Children'][1]['Children'][1]['Children'][0]['Max'])
        print(temp_min)
        print(temp_value)
        print(temp_max)

        temp_min2 = temp_min * 0.92
        temp_min3 = temp_min * 1.10

        temp_value2 = temp_value * 0.92
        temp_value3 = temp_value * 1.10

        temp_max2 = temp_max * 0.92
        temp_max3 = temp_max * 1.10

        if temp_min > 100:
                temp_min = 100

        if temp_min2 > 100:
                temp_min2 = 100

        if temp_min3 > 100:
                temp_min3 = 100

        if temp_value > 100:
                temp_value = 100

        if temp_value2 > 100:
                temp_value2 = 100

        if temp_value3 > 100:
                temp_value3 = 100

        if temp_max > 100:
                temp_max = 100
  
        if temp_max2 > 100:
                temp_max2 = 100
  
        if temp_max3 > 100:
                temp_max3 = 100
  
  
                #CURSOR
        cursorLocal = db_connection.cursor()
        cursorLocal2 = db_connection.cursor()
        cursorAzure = cnxn.cursor()
        cursorAzure2 = cnxn.cursor()
                        

                        
                        #TEMPERATURA AZURE

        fkAtm = 1                
        dataHora = datetime.now()
        dataHoraFormat = dataHora.strftime('%Y/%m/%d %H:%M:%S')
        cursorAzure.execute("INSERT INTO Temperatura (fkAtm, tMin, tMed, tMax, dataHora ) VALUES (?,?,?,?,?);",
         (1, temp_min, temp_value, temp_max, dataHoraFormat))
        # values = (temp_min, temp_value, temp_max, dataHoraFormat)
        # cursorAzure2.execute(sqltemp, values)

        fkAtm = 2                
        dataHora = datetime.now()
        dataHoraFormat = dataHora.strftime('%Y/%m/%d %H:%M:%S')
        cursorAzure.execute("INSERT INTO Temperatura (fkAtm, tMin, tMed, tMax, dataHora ) VALUES (?,?,?,?,?);",
         (2, temp_min2, temp_value2, temp_max2, dataHoraFormat))

        fkAtm = 3                
        dataHora = datetime.now()
        dataHoraFormat = dataHora.strftime('%Y/%m/%d %H:%M:%S')
        cursorAzure.execute("INSERT INTO Temperatura (fkAtm, tMin, tMed, tMax, dataHora ) VALUES (?,?,?,?,?);",
         (3, temp_min3, temp_value3, temp_max3, dataHoraFormat))
        

                        #TEMPERATURA LOCAL
        fkAtm = 1
        dataHora = datetime.now()
        dataHoraFormat = dataHora.strftime('%Y-%m-%d %H:%M:%S')
        sqltemp = ("INSERT INTO Temperatura (fkAtm, tMin, tMed, tMax, dataHora ) VALUES (%s,%s,%s,%s,%s)")
        values = (1, temp_min, temp_value, temp_max, dataHoraFormat)
        cursorLocal.execute(sqltemp, values) 

        fkAtm = 2
        dataHora = datetime.now()
        dataHoraFormat = dataHora.strftime('%Y-%m-%d %H:%M:%S')
        sqltemp = ("INSERT INTO Temperatura (fkAtm, tMin, tMed, tMax, dataHora ) VALUES (%s,%s,%s,%s,%s)")
        values = (2, temp_min2, temp_value2, temp_max2, dataHoraFormat)
        cursorLocal.execute(sqltemp, values)

        fkAtm = 3
        dataHora = datetime.now()
        dataHoraFormat = dataHora.strftime('%Y-%m-%d %H:%M:%S')
        sqltemp = ("INSERT INTO Temperatura (fkAtm, tMin, tMed, tMax, dataHora ) VALUES (%s,%s,%s,%s,%s)")
        values = (3, temp_min3, temp_value3, temp_max3, dataHoraFormat)
        cursorLocal.execute(sqltemp, values)

        print("\n")
        print(cursorAzure.rowcount, "Inserindo no banco (Azure).")
        cnxn.commit()

        print(cursorLocal.rowcount, "Inserindo no banco (Local).")
        db_connection.commit()
        time.sleep(1)
