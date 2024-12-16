import pandas as pd # type: ignore
import numpy as np
import re
import psycopg2 # type: ignore
from psycopg2 import OperationalError, errorcodes, errors  # type: ignore


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
    
    try:
        # Creación tabla supermercados
        query_creacion_actores = """create table if not exists actores (
                            id_actor serial primary key,
                            nombre varchar(500) not null,
                            anio integer not null,
                            conocido_por varchar(500),
                            que_hace varchar(500),
                            premios varchar(500)
                            );"""

        cursor.execute(query_creacion_actores)
        conexion.commit()

        # Creación tabla tipo_producto
        query_creacion_tipo_generos = """create table if not exists generos (
                                id_genero serial primary key,
                                nombre varchar(100) not null
                                );"""

        cursor.execute(query_creacion_tipo_generos)
        conexion.commit()

        # Creación tabla marca
        query_creacion_peliculas = """create table if not exists peliculas (
                                id_pelicula varchar(100) primary key,
                                titulo varchar(100) not null,
                                tipo varchar(100) not null,
                                id_genero integer not null,
                                anio integer,
                                mes integer,
                                nota numeric(8,4),
                                duracion varchar(100),
                                directores varchar(100),
                                guionistas varchar(100),
                                argumento varchar(1000),
                                foreign key (id_genero) references generos(id_genero)
                                );"""

        cursor.execute(query_creacion_peliculas)
        conexion.commit()

        # Creación tabla comparativa
        query_creacion_apariciones = """create table if not exists apariciones (
                                id_aparicion serial primary key,
                                id_pelicula varchar(100) not null,
                                id_actor integer not null,
                                foreign key (id_pelicula) references peliculas(id_pelicula),
                                foreign key (id_actor) references actores(id_actor)
                                );"""

        cursor.execute(query_creacion_apariciones)
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


