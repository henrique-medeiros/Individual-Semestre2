from json import loads
from time import sleep
from urllib3 import PoolManager
import mysql.connector

def conversor(valor):
    return float(valor[0:4].replace(",", '.'))

with PoolManager() as pool:
    while True:
        response = pool.request('GET', 'http://localhost:8090/data.json')
        data = loads(response.data.decode('utf-8'))
        temp_min = data['Children'][0]['Children'][1]['Children'][1]['Children'][0]['Min']
        temp_value = data['Children'][0]['Children'][1]['Children'][1]['Children'][0]['Value']
        temp_max = data['Children'][0]['Children'][1]['Children'][1]['Children'][0]['Max']

        #######################################################################

        print('--'*10)
        tmin = temp_min[0] + temp_min [1]
        tnormal = temp_value[0] + temp_value [1]
        tmax = temp_max[0] + temp_max [1]

        #######################################################################

        temp_minimo = float(tmin)
        temp_normal = float(tnormal)
        temp_maximo = float(tmax)
        dataHora = datetime.now()
        dataHoraFormat = dataHora.strftime('%Y/%m/%d %H:%M:%S')

        def criarTabela():

            config = {
                'user': 'root',
                'password': '#Gf47139014825',
                'host': 'localhost',
                'database': 'REC',
                'raise_on_warnings': True
            }

            cnx = mysql.connector.connect(**config)

            if cnx.is_connected():
                db_info = cnx.get_server_info()
                print('conectado', db_info)
                cursor = cnx.cursor()
                cursor.execute("select database();")
                linha = cursor.fetchone()
                print("Conectado ao banco de dados:", linha)

            cursor = cnx.cursor()
            sql = ("INSERT INTO Temperatura (tMin, tMed, tMax, dataHora) VALUES (%s,%s,%s,%s)")
            values = (temp_minimo, temp_normal, temp_maximo, dataH)

            try:
                cursor.execute(sql, values)

                cnx.commit()

                print("Dados Inseridos com sucesso")

            except mysql.connector.Error as err:
                cnx.rollback()
                print("Algo deu errado: {}".format(err))

            cnx.close()

            sleep(1)

        criarTabela()