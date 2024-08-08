import pandas as pd
import requests
from datetime import datetime, timedelta

def convert_to_unix_timestamp(dt):
    unix_timestamp = int(dt.timestamp())
    return unix_timestamp

def getBlockByTimestamp(api_key, timestamp, endpoint):
    params = {
        'module': 'block',
        'action': 'getblocknobytime',
        'timestamp': timestamp,
        'closest': 'before',
        'apikey': api_key
    }
    
    response = requests.get(endpoint, params=params)
    
    if response.status_code == 200:
        data = response.json()
        if data['status'] == '1':
            return data['result']
        else:
            return f"Error: {data['message']}"
    else:
        return f"HTTP Error: {response.status_code}"


def convert_to_bsas_time(timestamp):
    date_time = datetime.fromtimestamp(timestamp) - timedelta(hours=3)
    return date_time.strftime('%Y-%m-%d %H:%M:%S')


apiKeys = {
    "BSC": "V5HFRXY9H2EFW9XGV5FEXHJ8QWWXIBBCTY",
    "ETH": "CFMFXZ6TRDRX1VEY1RJBW6IF4YH6HQEXRP",
    "POLYGON": "VGNCWFUNQC5RMMKEIMYE9CGHYFAGUGPJJY",
}

endpoints = {"BSC" : "https://api.bscscan.com/api",
             "ETH" : "https://api.etherscan.io/api",
             "POLYGON" : "https://api.polygonscan.com/api"
             }

params = {"paramsBsc" : {
    "module": "account",
    "action": "tokentx",
    "page": 1,
    "offset": 10000,
    "sort": "desc",
    "apikey": apiKeys["BSC"]
    },
          "paramsEth" : {
    "module": "account",
    "action": "tokentx",
    "sort": "desc",
    "apikey": apiKeys["ETH"]
    },

           "paramsPolygon" : {
    "module": "account",
    "action": "tokentx",
    "page": 1,
    "offset": 10000,
    "sort": "desc",
    "apikey": apiKeys["POLYGON"]
    }
}

wallets = {
    'w1': {
        'direccion': '0x8894E0a0c962CB723c1976a4421c95949bE2D4E3',
        'red': ["ETH"]
    },
    'w2': {
         'direccion': '0x1514Cd1d63d304D40574Fc33a61E8A2a202c1EeB',
         'red': ["BSC", "POLYGON"]
    },
     'w3': {
         'direccion': '0x8894E0a0c962CB723c1976a4421c95949bE2D4E3',
         'red': ["BSC"]
     }
}

contratos = {
    "BSC" : [
          "0x55d398326f99059ff775485246999027b3197955",
	    "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",
	    "0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3",
	    "0xcc42724c6683b7e57334c4e856f4c9965ed682bd",
	    "0x23396cf899ca06c4472205fc903bdb4de249d6fc",
	    "0x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c",
	    "0x2170ed0880ac9a755fd29b2688956bd959f933f8",
        "0xe9e7cea3dedca5984780bafc599bd69add087d56",
	    "0x570a5d26f7765ecb712c0924e4de545b89fd43df"
    ],
    "ETH" : [
        "0x6b175474e89094c44da98b954eedeac495271d0f",
	    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
	    "0xdb25f211ab05b1c97d595516f45794528a807ad8",
	    "0xa47c8bf37f92abed4a126bda807a7b7498661acd",
	    "0xdAC17F958D2ee523a2206206994597C13D831ec7",
	    '0x4Fabb145d64652a948d72533023f6E7A623C7C53',
	    "0x83F20F44975D03b1b09e64809B757c47f942BEeA",
	    '0xc3d688B66703497DAA19211EEdff47f25384cdc3'

    ],
    "POLYGON" : [
        "0xc2132D05D31c914a87C6611C10748AEb04B58e8F",
	    "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",
	    "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
    ]
}




def obtenerTransacciones3Redes(wallets, fechaDesde, fechaHasta):
    dfs = pd.DataFrame()
    all_transactions = [] 
    paramsActuales = {}
    endpoint = ""
    contratosActuales = []


    for walletId, walletInfo in wallets.items():
        print(f"Wallet ID: {walletId}")
        print(f"Direccion: {walletInfo['direccion']}")
        print(f"Red: {walletInfo['red']}")
        print()



        for red_actual in walletInfo['red']:
            if red_actual == 'BSC':
                paramsActuales = params["paramsBsc"].copy()
                endpoint = endpoints["BSC"]
                contratosActuales = contratos["BSC"]
                print (contratosActuales)
                print("se eligio red BSC")
            elif red_actual == 'ETH':
                paramsActuales = params["paramsEth"]
                endpoint = endpoints["ETH"]
                contratosActuales = contratos["ETH"]
                print("se eligio red ETH")
            elif red_actual == 'POLYGON':
                paramsActuales = params["paramsPolygon"]
                endpoint = endpoints["POLYGON"]
                contratosActuales = contratos["POLYGON"]
                print("se eligio red POLY")
            else:
                print("Red no válida")
                continue
            
            try:
                timestampDesde = convert_to_unix_timestamp(fechaDesde)
                print(timestampDesde)
                block_number_Desde = getBlockByTimestamp(paramsActuales['apikey'], timestampDesde, endpoint)
                print("Block number Desde:", block_number_Desde)
            except Exception as e:
                print("Ocurrió un error al obtener el número de bloque para la fechaDesde:", e)

            try:
                timestampHasta = convert_to_unix_timestamp(fechaHasta)
                print(timestampHasta)
                block_number_Hasta = getBlockByTimestamp(paramsActuales['apikey'], timestampHasta, endpoint)
                print("Block number Hasta:", block_number_Hasta)
            except Exception as e:
                print("Ocurrió un error al obtener el número de bloque para la fechaHasta:", e)
            
            paramsActuales['address'] = walletInfo['direccion']
            paramsActuales['startblock'] = block_number_Desde
            paramsActuales['endblock'] =  block_number_Hasta

            dfActual = getTransaccionesEntreFechas(all_transactions, endpoint, paramsActuales, block_number_Hasta, block_number_Desde, timestampDesde, contratosActuales)
            if not ( dfActual.empty ):
                dfActual.insert(0, 'Wallet ID', walletId)  
                dfActual.insert(1, 'Red Actual', red_actual)   
                dfActual.insert(2, 'Dirección', walletInfo['direccion'] )
                dfs = pd.concat([dfs, dfActual], ignore_index=True)
    print(dfs.columns)
    dfs.insert(3, 'FechaBsAs', dfs['timeStamp'].astype(int).apply(convert_to_bsas_time))
    dfs = dfs.drop(columns=['confirmations'])
    dfs = dfs.drop_duplicates(subset=['Wallet ID', 'Red Actual', 'Dirección', 'FechaBsAs', 'blockNumber', 'timeStamp', 'gas', 'gasPrice', 'gasUsed', 'cumulativeGasUsed', 'input'])

    return dfs


def getTransaccionesEntreFechas(all_transactions, endpoint, paramsActuales, block_number_Hasta, block_number_Desde,  timestampDesde, contratosActuales):
    all_transactions = []
    print("Params actuales:", paramsActuales)
    
    if (block_number_Desde != 'errorNOTOK' and block_number_Hasta != 'errorNOTOK'):
        print("entro al if")

        while True:
            response = requests.get(endpoint, params=paramsActuales).json()
            transactions = response['result']
            print(len(transactions))
            if not transactions:
                print("no habia transacciones")
                break

            last_tx_timestamp = int(transactions[-1]['timeStamp'])
            
            transactions = [tx for tx in transactions if tx['contractAddress'] in contratosActuales]
            
            if last_tx_timestamp <= timestampDesde:
                print("el timestamp fue menor que la fecha minima")
                filtered_transactions = [tx for tx in transactions if int(tx['timeStamp']) > timestampDesde]
                all_transactions.extend(filtered_transactions)
                break
            
            if not transactions:
                print("se filtraron la transacciones y no quedo ninguna")
                break

            ultimoBlockNumber = int(transactions[-1]['blockNumber'])

            if ultimoBlockNumber == paramsActuales['endblock']:
                print("No se avanzó al siguiente bloque. Rompiendo el ciclo para evitar bucle infinito.")
                break

            paramsActuales['endblock'] = ultimoBlockNumber
            all_transactions.extend(transactions)
            print(ultimoBlockNumber)
        return pd.DataFrame(all_transactions)
    else:
        print("No hay bloques de transacciones en el rango de fechas especificado para esta RED.")
    



fechaDesde = datetime(2024, 6, 6, 00, 00) + timedelta(hours=3)
fechaHasta = datetime(2024, 6, 6 , 19, 00) + timedelta(hours=3)

resultadoDf = obtenerTransacciones3Redes(wallets, fechaDesde, fechaHasta);
print(resultadoDf.head())
print(resultadoDf.tail())
print(resultadoDf.columns)

#test
if not resultadoDf.empty:
    first_tx_timestamp = int(resultadoDf.iloc[0]['timeStamp'])
    last_tx_timestamp = int(resultadoDf.iloc[-1]['timeStamp'])

    print(first_tx_timestamp)
    print(last_tx_timestamp)
else:
    print("No transactions found in the specified date range.")


resultadoDf.to_csv('final.csv', index=False)