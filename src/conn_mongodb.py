import os
import pandas as pd
from pymongo import MongoClient


# Crea una instancia de MongoClient que se conecta al servidor de MongoDB que se encuentra en 'localhost' (la máquina local)
client = MongoClient('localhost')
# Accede a la base de datos llamada 'supertienda'. Si no existe, MongoDB la creará cuando se inserte algún dato en ella.
db = client['supertienda']
# Accede a la colección llamada 'ordenes' dentro de la base de datos 'supertienda'. 
# Si la colección no existe, MongoDB la creará cuando se inserte algún documento en ella.
col = db['orders']

# Ruta del archivo CSV
FILE_PATH = os.path.join("dataset","superstore.csv")

# Carga del archivo CSV a Pandas
df = pd.read_csv(FILE_PATH, encoding="latin-1")


documents = []
for index, row in df.iterrows():
    document = {
        "OrderID": row['Order ID'],
        "OrderDate": row['Order Date'],
        "ShipMode": row['Ship Mode'],
        "CustomerID": row['Customer ID'],
        "CustomerName": row['Customer Name'],
        "Segment": row['Segment'],
        "Country": row['Country'],
        "City": row['City'],
        "State": row['State'],
        "PostalCode": row['Postal Code'],
        "Region": row['Region'],
        "ProductID": row['Product ID'],
        "Category": row['Category'],
        "SubCategory": row['Sub-Category'],
        "ProductName": row['Product Name'],
        "Sales": row['Sales'],
        "Quantity": row['Quantity'],
        "Discount": row['Discount'],
        "Profit": row['Profit']
    }
    documents.append(document)

# Insertar los documentos en la colección, evitando duplicados.
try: 
    if documents:  # Verifica que la lista de documentos no esté vacía
        for doc in documents:
            # Verificar si el documento ya existe basado en múltiples campos, por ejemplo, 'OrderID' y 'CustomerID'
            query = {
                "OrderID": doc["OrderID"],
                "CustomerID": doc["CustomerID"],
                "ProductID": doc["ProductID"],
                "Sales": doc["Sales"]
            }
            if col.count_documents(query) == 0:  # Si no existe ningún documento con esos campos
                col.insert_one(doc)  # Insertar el documento
        print("Documentos insertados.")

except Exception as e:
    print(f"Error al insertar documentos: {e}")
