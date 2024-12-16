import pandas as pd # type: ignore
import numpy as np
import re
import psycopg2 # type: ignore
from psycopg2 import OperationalError, errorcodes, errors  # type: ignore

def obtener_subcategoria(row):

    """Extraemos de cada fila una subcategoría en función del nombre del producto

    Args:
        row: fila del dataframe

    Returns:
        Devuelve una string en caso de satisfacer algunas de las condiciones, de lo contario un Nan
    """

    if row['producto'] == 'aceite de oliva':
        if 'virgen extra' in row['nombre_producto']:
            return 'virgen extra'
        elif 'virgen' in row['nombre_producto']:
            return 'virgen'
        else:
            return np.nan
    elif row['producto'] == 'leche':
        if 'entera' in row['nombre_producto']:
            return 'entera'
        elif 'semi' in row['nombre_producto'] or 'semidesnatada' in row['nombre_producto']:
            return 'semidesnatada'
        elif 'desnatada' in row['nombre_producto']:
            return 'desnatada'
        else:
            return np.nan
    elif row['nombre_producto'] == 'aceite de girasol':
        return np.nan
    else:
        return np.nan
    
def extraer_marca_cantidad(nombre):
    
    """A partir del nombre de un producto extraemos su marca y cantidad.

    Args:
        nombre (str): nombre del producto.

    Returns:
        series: una serie con los valores almacenados. 
    """

    marcas_conocidas = ["hacendado", "auchan", "picual casa juncal", "capicua", "carrefour", "coosol", "fontasol", "koipe", "koipesol", "campomar", "ozolife",
                     "la masia", "ybarra", "carbonell", "abaco", "la espanola", "aromas del sur", "natursoy", "dcoop", "arguinano", "oro bailen",
                     "capricho andaluz", "coosur", "de nuestra tierra", "maestros de hojiblanca", "oro de genave", "marques de grinon", "nunez de prado", "oleoestepa",
                      "retama", "maeva", "cantero de letur", "pascual", "puleva", "asturiana", "kaiku", "lauki", "president", "rio", "nestle", "el buen pastor",
                       "danone", "laban", "actimel", "danacol", "noe", "denenes", "blemil", "almiron", "nivea", "ecran", "covap", "delial", "eroski",
                        "borgesol", "borges", "lanisol", "olilan", "urzante", "senorio de segura", "palacio de los olivos", "jaencoop", "carapelli",
                        "cazorliva", "duernas", "oro de genabe", "arrolan", "mendia", "olivar de segura", "saqura", "guillen", "mil olivas", "trujal tudela",
                        "o de la ribera", "sierra de cazorla", "guzman", "elizondo", "ultzama", "beyena", "bomilk", "celta", "euskal herria", "lacturale", 
                        "ram", "bizkaia", "ideal", "lactebal", "gaza", "flora", "diasol", "la almazara del olivar", "dia lactea", "abrisol", 
                        "el corte ingles", "elosol", "primer dia de cosecha", "abril", "casa juncal", "aceites de ardales", "agus", "alhema de queiles",
                        "aljibes", "almaoliva", "amarga y pica", "arboleda", "campo rico", "casas de hualdo", "castillo de canena",
                        "changlot", "conde de benalua", "dominus", "dulcesol", "el lagar del soto", "ester sole", "ferrarini", "finca penamoucho",
                        "flor de arana", "fuenroble", "germanor", "go vegg", "gotas de abril",  "hacienda el palo", "iznaoliva", "jacoliva", "lestornell",
                        "la almazara de canjayar", "la boella", "la laguna de fuente de piedra", "la organic", "la redonda", "hojiblanca", "merula", 
                        "misko", "miro", "molino de la calzada", "molino de olivas de bolea", "mueloliva", "nunez de prado", "oleaurum", "oleodiel",
                        "olibeas", "oliva verde", "olivar del sur", "pago baldios san carlos", "pan de olivo", "parqueoliva", "picualia", 
                        "reales almazaras de alcaniz", "romanico", "santiveri", "somontano", "surinver", "torremilano", "tresces", "unio", "alberto chicote",
                        "valroble", "venta del baron", "altamira", "ato", "clesa", "ecomil", "el castillo", "feiraco", "granja noe", "hipp", "la colmenarena",
                        "la yerbera", "lar", "larsa", "letona", "leyma", "lilibet", "llet nostra", "lletera campllong", "madriz", "pano", "priegola",
                        "senorio de sarria", "tierra de sabor", "unicla", "valles unidos", "villacorona", "cexasol", "ondosol", "alcampo", "ucasol", 
                        "el molino d gines", "fruto del sur", "giralda", "mar de olivos", "monegros", "olivar centenario", "olivo de cambil", "ondoliva",
                        "oro aragon", "oro virgen", "saeta", "suroliva", "valdezarza", "verde segura", "duc", "lr", "la colmenarena", "mntbelle", 
                        "santa gadea", "consorcio", "lorea", "santa teresa", "naturgreen", "mustela", "babaria", "babybio", "sveltesse", "saha", "arronizarbe"]
  
    marca = next((m for m in marcas_conocidas if m in nombre.lower()), np.nan) #va iterando por la lista de marcas y si no coincide ninguna pone nan
    
    cantidad = re.search(r"(\d+(\.\d+)?\s*(x\s*\d+\s*)?[ml|l|cl|g|mg|kg]+)", nombre.lower())
    cantidad = cantidad.group(0) if cantidad else np.nan #Devuelve la cadena macheada por la re
    df = pd.Series([marca, cantidad])
    return df

def conexion_bbdd(nombre):
    
    """Establece una conexión a una base de datos

    Args:
        nombre (str): nombre de la base de datos

    Returns:
        conexion (psycopg2.extensions.connection): Objeto de conexión a la base de datos PostgreSQL.
    """

    try:  
        conexion = psycopg2.connect(
        database = nombre,
        user = "postgres",
        password = "admin",
        host = "localhost",
        port = "5432")
        
    except OperationalError as e:
        if e.pgcode == errorcodes.INVALID_PASSWORD:
            print("La contraseña es erronea")
        elif e.pgcode == errorcodes.CONNECTION_EXCEPTION:
            print("Error de conexion")
        else:
            print(f"Ocurrió el error {e}")

    return conexion



def creacion_tablas(conexion, cursor):
    
    """Crea las tablas necesarias en la base de datos si no existen.

    Las tablas creadas son:
    - `supermercados`: Contiene la información de supermercados, con `id_supermercado` como clave primaria.
    - `tipo_producto`: Contiene información sobre los tipos de productos, con `id_producto` como clave primaria.
    - `marcas`: Contiene las marcas de productos, con `id_marca` como clave primaria.
    - `comparativa`: Almacena información comparativa de productos, con referencias a las tablas `supermercados`, 
      `tipo_producto` y `marcas`. Incluye detalles como el precio, fecha y porcentaje de incremento.

    Args:
        conexion (psycopg2.extensions.connection): Objeto de conexión a la base de datos.
        cursor (psycopg2.extensions.cursor): Objeto de cursor para ejecutar comandos SQL.

    Raises:
        Exception: Si ocurre un error en la creación de las tablas, imprime el mensaje de error.
    
    """

    try:
        # Creación tabla supermercados
        query_creacion_supermercado = """create table if not exists supermercados (
                            id_supermercado int primary key,
                            nombre varchar(100) not null
                            );"""

        cursor.execute(query_creacion_supermercado)
        conexion.commit()

        # Creación tabla tipo_producto
        query_creacion_tipo_producto = """create table if not exists tipo_producto (
                                id_producto int primary key,
                                nombre varchar(100) not null
                                );"""

        cursor.execute(query_creacion_tipo_producto)
        conexion.commit()

        # Creación tabla marca
        query_creacion_marca = """create table if not exists marcas (
                                id_marca numeric(8,4) primary key,
                                nombre varchar(100) not null
                                );"""

        cursor.execute(query_creacion_marca)
        conexion.commit()

        # Creación tabla comparativa
        query_creacion_comparativa = """create table if not exists comparativa (
                                id_comparativa serial primary key,
                                id_supermercado integer not null,
                                id_producto integer not null,
                                nombre varchar(300) not null,
                                tipo varchar(200),
                                id_marca numeric(8,4),
                                fecha date not null,
                                dia_semana varchar(100) not null,
                                precio numeric(8,4),
                                cantidad varchar(100),
                                incremento varchar(50),
                                porcentaje numeric(8,4),
                                foreign key (id_supermercado) references supermercados(id_supermercado),
                                foreign key (id_producto) references tipo_producto(id_producto),
                                foreign key (id_marca) references marcas(id_marca)
                                );"""

        cursor.execute(query_creacion_comparativa)
        conexion.commit()

    except Exception as e:
        print(f"Error creando tablas: {e}")


def insercion_supermercados(conexion, cursor):
    
    """Inserta registros de supermercados en la tabla `supermercados` de la base de datos.

    Lee datos desde un archivo CSV llamado `tabla_super.csv` y los inserta en la tabla `supermercados`.

    Args:
        conexion (psycopg2.extensions.connection): Objeto de conexión a la base de datos.
        cursor (psycopg2.extensions.cursor): Objeto de cursor para ejecutar comandos SQL.
    """

    df_tabla_super = pd.read_csv("../datos/tabla_super.csv", index_col=0)
    lista_tuplas_super = [tuple(fila) for fila in df_tabla_super.values]
    query_insercion = "insert into supermercados (id_supermercado, nombre) values (%s, %s)"
    cursor.executemany(query_insercion, lista_tuplas_super)
    conexion.commit()

def insercion_productos(conexion, cursor):

    """"Inserta registros de productos en la tabla `tipo_producto` de la base de datos.

    Lee datos desde un archivo CSV llamado `tabla_productos.csv` y los inserta en la tabla `tipo_producto`.

    Args:
        conexion (psycopg2.extensions.connection): Objeto de conexión a la base de datos.
        cursor (psycopg2.extensions.cursor): Objeto de cursor para ejecutar comandos SQL.

    """

    df_tabla_productos = pd.read_csv("../datos/tabla_productos.csv", index_col=0)
    lista_tuplas_productos = [tuple(fila) for fila in df_tabla_productos.values]
    query_insercion = "insert into tipo_producto (id_producto, nombre) values (%s, %s)"
    cursor.executemany(query_insercion, lista_tuplas_productos)
    conexion.commit()

def insercion_marcas(conexion, cursor):

    """Inserta registros de marcas en la tabla `marcas` de la base de datos.

    Lee datos desde un archivo CSV llamado `tabla_marcas.csv` y los inserta en la tabla `marcas`.

    Args:
        conexion (psycopg2.extensions.connection): Objeto de conexión a la base de datos.
        cursor (psycopg2.extensions.cursor): Objeto de cursor para ejecutar comandos SQL.
    """

    df_tabla_marcas = pd.read_csv("../datos/tabla_marcas.csv", index_col=0)
    lista_tuplas_marcas = [tuple(fila) for fila in df_tabla_marcas.values]
    query_insercion = "insert into marcas (id_marca, nombre) values (%s, %s)"
    cursor.executemany(query_insercion, lista_tuplas_marcas)
    conexion.commit()

def insercion_comparativa(conexion, cursor):

    """
    Inserta registros de comparativas de productos en la tabla `comparativa` de la base de datos.

    Lee datos desde un archivo CSV llamado `tabla_comparativa.csv` y los inserta en la tabla `comparativa`.
    Reemplaza valores nulos en la columna `id_marca` con `None` para la correcta inserción en la base de datos.

    Args:
        conexion (psycopg2.extensions.connection): Objeto de conexión a la base de datos.
        cursor (psycopg2.extensions.cursor): Objeto de cursor para ejecutar comandos SQL.

    """
    df_tabla_comparativa = pd.read_csv("../datos/tabla_comparativa.csv", index_col=0)
    df_tabla_comparativa['id_marca'] = df_tabla_comparativa['id_marca'].replace(np.nan, None)
    lista_tuplas_comparativas = [tuple(fila) for fila in df_tabla_comparativa.values]
    query_insercion = """insert into comparativa 
        (id_supermercado, id_producto, nombre, tipo, fecha, precio, incremento, porcentaje, cantidad, dia_semana, id_marca) 
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    cursor.executemany(query_insercion, lista_tuplas_comparativas)
    conexion.commit()


