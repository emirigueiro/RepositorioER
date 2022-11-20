import os
import re as re
from collections import OrderedDict

import numpy as np
import pandas as pd

cwd = os.getcwd() 
files = os.listdir(cwd)  
cwd


#Import CSV File.
MIDDLE = pd.read_csv('MIDDLE_CISA_JUNIO.csv', sep=";", encoding='latin-1')

#File Validation: función cantidad de columnas y nombre de columnas.

def CANTIDAD_COLUMNAS(x):
    """_summary_: Valida el archivo de carga contra el formato "tipo" enviado por el socio y realiza una primera validación sobre el número de columnas.
                  Para validar se toma la lista de campos "standar" definida en la linea 3.
    Args:
         (DataFrame): generado a partir de un CSV enviado por el socio.

    Returns:
         Devuelve un mensaje solo cuando se identifica que cambio (se agrega o se quita) el número de columnas.
    """  
    
# Se definen las listas de campos a comparar (archivo de orginal) vs (nuevo archivo)
    Columnas = x.columns
    Columnas_Original = ['Apellido', 'Nombre', 'NumeroDocumento', 'Calle', 'Altura', 'Piso',
       'Departamento', 'Ciudad', 'Provincia', 'CodigoPostal', 'NumeroZona',
       'Numero', 'Email', 'CodigoIA', 'Marca', 'Modelo', 'idUso',
       'NumeroMotor', 'NumeroChasis', 'Patente', 'Año', 'SumaAsegurada',
       'Compañía', 'Cobertura', 'Monto', 'EstadoContrato4NH', 'idAutomovil',
       'FechaFin', 'Observaciones', '1º Llamado', '2º Llamado', '3º Llamado',
       '4º Llamado', 'Observacion'] 

    if len(Columnas_Original) != len(Columnas):
        return ('Cambio el número de columnas')

def NOMBRE_COLUMNAS(x):
    """_summary_: Valida el archivo de carga contra el formato "tipo" enviado por el socio y realiza una segunda validación sobre el nombre de cada columna.

    Args:
        (DataFrame): generado a partir de un CSV enviado por el socio.

    Returns:
      Devuelve un mensaje solo cuando se identifica que cambio el nombre de alguna columna.
    """      
# Se definen las listas de campos a comparar (archivo de orginal) vs (nuevo archivo)
    Columnas = x.columns
    Columnas_Original = ['Apellido', 'Nombre', 'NumeroDocumento', 'Calle', 'Altura', 'Piso',
       'Departamento', 'Ciudad', 'Provincia', 'CodigoPostal', 'NumeroZona',
       'Numero', 'Email', 'CodigoIA', 'Marca', 'Modelo', 'idUso',
       'NumeroMotor', 'NumeroChasis', 'Patente', 'Año', 'SumaAsegurada',
       'Compañía', 'Cobertura', 'Monto', 'EstadoContrato4NH', 'idAutomovil',
       'FechaFin', 'Observaciones', '1º Llamado', '2º Llamado', '3º Llamado',
       '4º Llamado', 'Observacion'] 

    for i in range(min(len(Columnas), len(Columnas_Original))):
        if Columnas[i] != Columnas_Original[i]:
            return ('cambio el nombre de alguna una columna')

# Normalización del campo "Observaciones".

def NORMALIZACION_CAMPO_OBSERVACIONES():
      """_summary_: Realiza un split del campo observaciones y luego itera sobre e mismo para limpiar palabras y caraceteres 
                    que no se van a utilizar para la generación de DF final.

      Args:
          x (DataFrame): generado a partir de un CSV enviado por el socio.
          y ('Campo'): se debe tomar el campo "Observaciones" que se encuentra entro de DataFrame "MIDDLE"

      Returns:
          Array: Devuelve el campo observaciones separado por espacion en un array limpio de caracteres y palabras solo con datos de telefonos e emails.
      """
      try:
          MIDDLE3 = MIDDLE['Observaciones'].str.split(" ", n = 25, expand = True)
          MIDDLE3.replace([None],[""], inplace = True)
          MIDDLE3.replace(['Teléfonos:'],[""], inplace = True)
          MIDDLE3.replace(['Telefonos:'],[""], inplace = True)
          MIDDLE3.replace(['Teléfono:'],[""], inplace = True)
          MIDDLE3.replace(['Telefonos:'],[""], inplace = True)
          MIDDLE3.replace(['Enriquecimiento'],[""], inplace = True)
          MIDDLE3.replace(['datos:'],[""], inplace = True)
          MIDDLE3.replace(['datos'],[""], inplace = True)
          MIDDLE3.replace(['Cel:'],[""], inplace = True)
          MIDDLE3.replace(['Tel:'],[""], inplace = True)
          MIDDLE3.replace(['de'],[""], inplace = True)
          MIDDLE3.replace(['Emails:'],[""], inplace = True)
          MIDDLE3.replace([','],[""], inplace = True)
          return MIDDLE3
      except:
          print('Se modifico el campo observaciones enviado en el archivo de origen')

# Normalización de los campos de Emails.

def NORMALIZACION_EMAILS(x):
    """_summary_: se utiliza la función NORMALIZACION_CAMPO_OBSERVACIONES para poder iterar sobre el campo correctamente. Luego se utiliza la función LIMPIEZA_EMAIL
                  para extraer los emails a partir de la indentifiación de un @. Luego se limpian los caracteres especiales que estan de mas. 

    Args:
        x (DataFrame): generado a partir de un csv enviado por el socio.

    Returns:
        DataFrame: DF ordenado en forma vertical con dato de tipo/número de documento y su respectivo email asociado. Si existe un 
                   documento con mas de un email, se repite el número de docuemnto. Se agrega indice autoincremental. Para esto se utiliza la función 
                   PREPARACION_CONCATENACION_VERTICAL. Por último se eliminan duplicados.            
    """    
    try:
        MIDDLE3 = NORMALIZACION_CAMPO_OBSERVACIONES()

        # EMAIL 1 Se renombra el campo "Email" por "Email_1"
        x.rename({'Email':'Email_1'}, axis=1, inplace=True)
        MIDDLE['Email_1'].replace(['NaN'],[""], inplace = True)
    
        # Email 2 y 3: limpieza aplicando la función LIMPIEZA_EMAIL
        MIDDLE['Email_2'] = LIMPIEZA_EMAIL('Email_2', 6, MIDDLE3)
        MIDDLE['Email_3'] = LIMPIEZA_EMAIL('Email_3', 4, MIDDLE3)
      
        # Creación DF Vertical MIDDLE_CONTACTO_EMAIL
        MIDDLE_EMAIL1 = PREPARACION_CONCATENACION_VERTICAL ('Email_1', 'Email')  
        MIDDLE_EMAIL2 = PREPARACION_CONCATENACION_VERTICAL ('Email_2', 'Email')
        MIDDLE_EMAIL3 = PREPARACION_CONCATENACION_VERTICAL ('Email_3', 'Email')
        MIDDLE_CONTACTO_EMAIL = pd.concat([MIDDLE_EMAIL1, MIDDLE_EMAIL2, MIDDLE_EMAIL3], axis=0)

        #Drop duplicates
        MIDDLE_CONTACTO_EMAIL = MIDDLE_CONTACTO_EMAIL.drop_duplicates(subset=['Email'], keep='last')

        # Tranformación a minuscula
        MIDDLE_CONTACTO_EMAIL['Email'] = MIDDLE_CONTACTO_EMAIL['Email'].str.low

        # Reseteo del Indice para que sea incremental
        MIDDLE_CONTACTO_EMAIL =  MIDDLE_CONTACTO_EMAIL.reset_index()
        del  MIDDLE_CONTACTO_EMAIL['index'] 
        return MIDDLE_CONTACTO_EMAIL   
    except:
        print('Cambio el campo "Email" o se modifico el campo "Obesrvaciones"')    

def LIMPIEZA_EMAIL (x, y, z):
    """Itera sobre el array resultante del split del campo "Observaciones" para encontrar un @. En caso que lo encuente lo inserta en un campo dentro del DF "MIDDLE"
       con el nombre "Emial_2" o "Email_3". Luego elimina el "." al final de cada email para dejarlo limpio.

    Args:
        x (Campo): Nombre del nuevo campo
        y (Campo): Posición dentro del campo "Observaciones"
        z (Campo): DF con el array resultante del split del campo "Observaciones"

    Returns:
        Campo: Campo Email dentro del DF MIDDLE con el dato de Email para cada cliente.
    """    
    MIDDLEBOOL = z[y].str.contains('@')
    MIDDLE[x] = ''
    MIDDLE['Email_sucio'] = z[y]
    MIDDLE['bool'] = MIDDLEBOOL
    # Toma solo aquellos emails que tengan un "@"
    MIDDLE[x] = MIDDLE['Email_sucio'].where(MIDDLE['bool'] == True, '')  
    A = []
    for i in MIDDLE[x]:
        resultado = (i[:-1]) 
        A.append(str(resultado))
    MIDDLE[x] = A
    return MIDDLE[x]
     
#Normalización de los campos de Teléfono.

def NORMALIZACION_TELEFONOS(x):
       """Se realiza la normalización de los 5 telefonos que llegan dentro del campo observaciones eliminando los caracteres especiales y renombrando los campos aplicando 
          la función LIMPIEZA_TELEFONOS:genera un split de cada número de teléfono, luego se eliminan caracteres especiales y finalmente se vuelve a concatenar.
  
       Args:
          MIDDLE (DataFrame):generado a partir de un csv enviado por el socio.

      Returns:
          DataFrame: devuelve un DF con los datos de tipo/número de documento, nombre, apellido y número de teléfono, repitiendo el documento/nombre en caso que alla mas de un telfono 
          para cada cliente, ordenado en forma vertical con indice incremental.    
       """    
       try:
         # Limpieza del Campo Observaciones 
         MIDDLE0 = NORMALIZACION_CAMPO_OBSERVACIONES()  

         #Rename del campo "numero por "Telefono_1
         MIDDLE.rename({'Numero':'Telefono_1'}, axis=1, inplace=True) 

         # Creación de campos de Telefono
         MIDDLE['Telefono_2'] = MIDDLE0[8]
         MIDDLE['Telefono_3'] = MIDDLE0[12]
         MIDDLE['Telefono_4'] = MIDDLE0[14]
         MIDDLE['Telefono_5'] = MIDDLE0[16]
         MIDDLE['Telefono_6'] = MIDDLE0[18]

         # Se aplica la función limpiar_telefonos especiales a cada campo de telefono
         MIDDLE['Telefono_1'] = LIMPIEZA_TELEFONOS (MIDDLE, 'Telefono_1')
         MIDDLE['Telefono_2'] = LIMPIEZA_TELEFONOS (MIDDLE, 'Telefono_2')
         MIDDLE['Telefono_3'] = LIMPIEZA_TELEFONOS (MIDDLE, 'Telefono_3')
         MIDDLE['Telefono_4'] = LIMPIEZA_TELEFONOS (MIDDLE, 'Telefono_4')
         MIDDLE['Telefono_5'] = LIMPIEZA_TELEFONOS (MIDDLE, 'Telefono_5')
         MIDDLE['Telefono_6'] = LIMPIEZA_TELEFONOS (MIDDLE, 'Telefono_6')

         # Creación de DF para concatenar verticalmente - Se utiliza la función PREPARACION_CONCATENACION_VERTICAL
         MIDDLE_TELA1 = PREPARACION_CONCATENACION_VERTICAL ('Telefono_1', 'Telefono')
         MIDDLE_TELA2 = PREPARACION_CONCATENACION_VERTICAL ('Telefono_2', 'Telefono')
         MIDDLE_TELA3 = PREPARACION_CONCATENACION_VERTICAL ('Telefono_3', 'Telefono')
         MIDDLE_TELA4 = PREPARACION_CONCATENACION_VERTICAL ('Telefono_4', 'Telefono')
         MIDDLE_TELA5 = PREPARACION_CONCATENACION_VERTICAL ('Telefono_5', 'Telefono')
         MIDDLE_TELA6 = PREPARACION_CONCATENACION_VERTICAL ('Telefono_6', 'Telefono')
         # Concatenación vertical
         MIDDLE_CONTACTO_TELEFONO = pd.concat([MIDDLE_TELA1, MIDDLE_TELA2, MIDDLE_TELA3, MIDDLE_TELA4, MIDDLE_TELA5, MIDDLE_TELA6], axis=0)

         #Drop duplicates
         MIDDLE_CONTACTO_TELEFONO = MIDDLE_CONTACTO_TELEFONO.drop_duplicates(subset=['Telefono'], keep='last')

         # Reseteo del Indice para que sea incremental
         MIDDLE_CONTACTO_TELEFONO = MIDDLE_CONTACTO_TELEFONO.reset_index()
         del MIDDLE_CONTACTO_TELEFONO['index'] 

         return MIDDLE_CONTACTO_TELEFONO
      
       except:
         print('Cambio la posición del dato de teléfono dentro del campo "Observaciones"')

def LIMPIEZA_TELEFONOS (x, y):
    """_summary_: Se realiza la limpieza de todos los tipos de caracteres especiales identificados en le campo que se defina. Para lo cual primero se realiza un split
                  y luego una concatenación.

    Args:
       x (DataFrame): DataFrame inicial (MIDDLE)
       y ('Campo'): Nombre del campo "Telefono" sobre el que se requiere realizarla limpieza

    Returns:
       DataFrame: Campo "Telefono" limpio de caracteres especiales.
    """   
    try:
      MIDDLETEL1 = (x[y].str.split("", n = 18, expand = True)) 
      MIDDLETEL1.replace(['-'],[""], inplace = True)
      MIDDLETEL1.replace(['_'],[""], inplace = True)
      MIDDLETEL1.replace(['.'],[""], inplace = True)
      MIDDLETEL1.replace([','],[""], inplace = True)
      MIDDLETEL1.replace([';'],[""], inplace = True)
      MIDDLETEL1.replace([')'],[""], inplace = True)
      MIDDLETEL1.replace(['('],[""], inplace = True)
      MIDDLETEL1.replace([None],[""], inplace = True)
      MIDDLETEL1.replace((['0'],['']), inplace = True)
      MIDDLE[y] = MIDDLETEL1[1] + MIDDLETEL1[2] + MIDDLETEL1[3] + MIDDLETEL1[4] + MIDDLETEL1[4] + MIDDLETEL1[5] + MIDDLETEL1[6] + MIDDLETEL1[7] + MIDDLETEL1[8] + MIDDLETEL1[9] + MIDDLETEL1[10] + MIDDLETEL1[11] + MIDDLETEL1[12] + MIDDLETEL1[13] 
      
      return MIDDLE[y]

    except:
      print('Cambio la posición del dato de teléfono dentro del campo "Observaciones"')   
    
def PREPARACION_CONCATENACION_VERTICAL (x, y):
   """_summary_: ordena los datos en forma vertical sumando número/tipo de docuemnto para cada telefono. En caso que exista mas de un teléfono para un número de documento, se repite
                 el número de documento. 
   Args:
       x ('Campo'): nombre del campo que se quiere ordenar en forma vertical, en este caso se debe tomar cada uno de los campos de teléfono.

   Returns:
       DataFrame: DF ordenado en forma vertical con datos de número/tipo de documento mas el teléfono.
   """   
   try:
     MIDDLE_TEL = pd.DataFrame()
     MIDDLE_TEL[y] = MIDDLE[x]
     MIDDLE_TEL['Tipo_Documento'] = MIDDLE['Tipo_Documento']
     MIDDLE_TEL['Nro_Documento'] = MIDDLE['Nro_Documento'] 
     
     return MIDDLE_TEL
   
   except:
      print('Hubo un error en la normalización de los datos personales (número o tipo de docuemnto)')

#Normalización de los campos de Documento y Modelo.

def NORMALIZACION_PERSONA_AUTO(x):
    """_summary_: Campo número de documento: se limpia de caracteres especiales.
                  Campo tipo de documento: se crea y se dejo vacio.
                  Campo modelo: se concatenan los campos marca y modelo, y se cambia el nombre del campo.
                  Campo patente: no se modifica nada, se trae como esta.
                  Se crea un DataFrame con datos de número/tipo de docuemnto, modelo y patente.

    Args:
        x (DataFrame): generado a partir del CSV enviado por el socio (MIDDLE).

    Returns:
        DataFrame: DataFrame con datos de número/tipo de docuemnto, modelo y patente.
    """ 
    try:   
      # Normalización Número y Tipo de Documento:
      # Transformación a STR
      MIDDLE['NumeroDocumento'] = x['NumeroDocumento'].astype(str)
      # Split para iterar
      DOC = (x['NumeroDocumento'].str.split("", n = 12, expand = True)) 
      # Limpieza de carcateres especiales
      DOC.replace(['.'],[""], inplace = True)
      DOC.replace([','],[""], inplace = True)
      DOC.replace(['-'],[""], inplace = True)
      DOC.replace([')'],[""], inplace = True)
      DOC.replace(['('],[""], inplace = True)
      DOC.replace([None],[""], inplace = True)
      # Se eliminan los "0" del final que general el split
      DOC[9].replace(['0'],[""], inplace = True)
      DOC[10].replace(['0'],[""], inplace = True)
      DOC[12].replace(['.0'],[""], inplace = True)
      x['NumeroDocumento'] = DOC[1] + DOC[2] + DOC[3] + DOC[4] + DOC[5]+ DOC[6] + DOC[7] + DOC[8] + DOC[9] + DOC[10] + DOC[11] + DOC[12]  
      x['NumeroDocumento'].replace(['nan'],[''], inplace = True)
      # Rename del campo NumeroDocumento
      x.rename({'NumeroDocumento':'Nro_Documento'}, axis=1, inplace=True)
      # Creación del Campo Tipo_Documento vacio
      x['Tipo_Documento'] = ""

      # Concatenación Marca y Model
      x['Modelo1'] = x['Marca'] + " " + x['Modelo']
      # Delete de Campos temporales
      del x['Marca']
      del x['Modelo']
      # Rename del campo Modelo
      x.rename({'Modelo1':'Modelo'}, axis=1, inplace=True)
      # Limpieza de campos temporales
    
      # Creación del DF vertical
      MIDDLE_PERSONA_AUTO = pd.DataFrame()
      MIDDLE_PERSONA_AUTO['Tipo_Documento'] = x['Tipo_Documento']
      MIDDLE_PERSONA_AUTO['Nro_Documento'] = x['Nro_Documento']
      MIDDLE_PERSONA_AUTO['Patente'] = x['Patente']
      MIDDLE_PERSONA_AUTO['Modelo'] = x['Modelo']
    
      return MIDDLE_PERSONA_AUTO

    except:
        print('Cambio la descripción el contenido de alguno de los siguientes campos: "NumeroDocumento" o "Modelo1"') 

#Ejecución


CANTIDAD_COLUMNAS(MIDDLE)
print(NOMBRE_COLUMNAS(MIDDLE))
print(NORMALIZACION_PERSONA_AUTO(MIDDLE))
print(NORMALIZACION_EMAILS(MIDDLE))
print(NORMALIZACION_TELEFONOS(MIDDLE))