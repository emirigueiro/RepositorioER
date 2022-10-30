"""The client request a transformation process in order to clean a csv file delivered for a partner with customer and product dates. 
       The process due to take a csv file clen all the dates that are not relevant for the Cliente´s CRM.
       The tranformation consists on identified and standardization diferents varables like telefon numbers, ID, email and product details.
       The out put of the process requered a Data frame with a vertical order of the dates, reapeting the ID when the customer haver
       more than one product or contact date (email or telefone) associated.-
"""

import pandas as pd
import numpy as np
import re as re
from collections import OrderedDict
import os

cwd = os.getcwd() 
files = os.listdir(cwd)  
cwd

def READ_CSV(x):
    return pd.read_csv(x, sep=";", encoding='latin-1')

FILE = READ_CSV('ORIGIN.csv')

def COLUMNS_NUMBER(FILE):
    """Valida el archivo de carga contra el formato "tipo" enviado con el socio y realiza una primera validación sobre el número de columnas.

    Args:
         CSV enviado por el socio.

    Returns:
            Devuelve un mensaje solo cuando se identifica que cambio (+ o -) el número de columnas.
    """  
    # Se definen las listas de campos a comparar (archivo de orginal) vs (nuevo archivo)
    Columnas = FILE.columns
    Columnas_Original = ['Apellido', 'Nombre', 'NumeroDocumento', 'Calle', 'Altura', 'Piso',
       'Departamento', 'Ciudad', 'Provincia', 'CodigoPostal', 'NumeroZona',
       'Numero', 'Email', 'CodigoIA', 'Marca', 'Modelo', 'idUso',
       'NumeroMotor', 'NumeroChasis', 'Patente', 'Año', 'SumaAsegurada',
       'Compañía', 'Cobertura', 'Monto', 'EstadoContrato4NH', 'idAutomovil',
       'FechaFin', 'Observaciones', '1º Llamado', '2º Llamado', '3º Llamado',
       '4º Llamado', 'Observacion'] 

    if len(Columnas_Original) != len(Columnas):
        return ('The columns number changed')

def COLUMNS_NAME(FILE):
    """Valida el archivo de carga contra el formato "tipo" enviado con el socio y realiza una segunda validación sobre el nombre de cada columna.

    Args:
      e_): CSV enviado por el socio.

    Returns:
      _type_: Devuelve un mensaje solo cuando se identifica que cambio el nombre de alguna columna.
    """  
    # Se definen las listas de campos a comparar (archivo de orginal) vs (nuevo archivo)
    Columnas = FILE.columns
    Columnas_Original = ['Apellido', 'Nombre', 'NumeroDocumento', 'Calle', 'Altura', 'Piso',
       'Departamento', 'Ciudad', 'Provincia', 'CodigoPostal', 'NumeroZona',
       'Numero', 'Email', 'CodigoIA', 'Marca', 'Modelo', 'idUso',
       'NumeroMotor', 'NumeroChasis', 'Patente', 'Año', 'SumaAsegurada',
       'Compañía', 'Cobertura', 'Monto', 'EstadoContrato4NH', 'idAutomovil',
       'FechaFin', 'Observaciones', '1º Llamado', '2º Llamado', '3º Llamado',
       '4º Llamado', 'Observacion'] 

    for i in range(min(len(Columnas), len(Columnas_Original))):
        if Columnas[i] != Columnas_Original[i]:
            return ('The columns name changed')


def EMAILS_TRANSFORMATION(FILE):
      """Se cambia el nombre del campo email, se normalizan los 2 emails que se encuentran dentro del campo observaciones, se transforma a STR
         Se elimina el "." del final de cada email para Email.

      Args:
          MIDDLE (_type_):Parte de un CSV enviado por el socio.
      """      
      # Limpieza del Campo Observaciones 
      MIDDLE3 = FILE['Observaciones'].str.split(" ", n = 25, expand = True)
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

    # Normalización de Emails

      # EMAIL 1
      # EMAIL 1 Se renombra el campo "Email" por "Email_1"
      FILE.rename({'Email':'Email_1'}, axis=1, inplace=True)

      # EMAIL 2
      # EMAIL 2 Se crea el booleano si hay "@"
      MIDDLEBOOL = MIDDLE3[6].str.contains('@')
      FILE['Email_2'] = ''
      FILE['Email_sucio'] = MIDDLE3[6]
      FILE['bool'] = MIDDLEBOOL
      # EMAIL 2 Se toma solo aquellos emails que tengan un "@"
      FILE['Email_2'] = FILE['Email_sucio'].where(FILE['bool'] == True, '')
      # Se eliminan campos temporales
      del FILE['Email_sucio']
      del FILE['bool']
      # Se eliminan los "." al final de los emails
      A = []
      for i in FILE['Email_2']:
            resultado = (i[:-1]) 
            A.append(str(resultado))
      FILE['Email_2'] = A

      # EMAIL 3 Se crea el booleano si hay @
      MIDDLEBOOL3 = MIDDLE3[4].str.contains('@')
      FILE['Email_3'] = ''
      FILE['Email_sucio_3'] = MIDDLE3[5]
      FILE['bool_3'] = MIDDLEBOOL3
      # EMAIL 3 Se toma solo aquellos emails que tengan un @
      FILE['Email_3'] = FILE['Email_sucio_3'].where(FILE['bool_3'] == True, '')
      # Se eliminan campos temporales
      del FILE['Email_sucio_3']
      del FILE['bool_3']

    # Normalización de Teléfonos

def TELEFONS_TRANFORMATION(FILE):
       """Primero se realiza split del campo observaciones y nos quedamos solo con los números de teléfono.
          Se realiza la normalización de los 5 telefonos que llegan dentro del campo sobservaciones eliminando los caracteres especiales y renombrando los campos.
          Para trabajar los datos primero se genera un split de cada número de teléfono, luego se eliminan caracteres especiales y finalmente se vuelve a concatenar.
  
       Args:
          MIDDLE (_type_):Parte de un CSV enviado por el socio.
       """    

       # Limpieza del Campo Observaciones 
       MIDDLE0 = FILE['Observaciones'].str.split(" ", n = 25, expand = True)
       MIDDLE0.replace([None],[""], inplace = True)
       MIDDLE0.replace(['Teléfonos:'],[""], inplace = True)
       MIDDLE0.replace(['Telefonos:'],[""], inplace = True)
       MIDDLE0.replace(['Teléfono:'],[""], inplace = True)
       MIDDLE0.replace(['Telefonos:'],[""], inplace = True)
       MIDDLE0.replace(['Enriquecimiento'],[""], inplace = True)
       MIDDLE0.replace(['datos:'],[""], inplace = True)
       MIDDLE0.replace(['datos'],[""], inplace = True)
       MIDDLE0.replace(['Cel:'],[""], inplace = True)
       MIDDLE0.replace(['Tel:'],[""], inplace = True)
       MIDDLE0.replace(['de'],[""], inplace = True)
       MIDDLE0.replace(['Emails:'],[""], inplace = True)
       MIDDLE0.replace([','],[""], inplace = True)   

       # Creación de campos de Telefono
       FILE['Telefono_2'] = MIDDLE0[8]
       FILE['Telefono_3'] = MIDDLE0[12]
       FILE['Telefono_4'] = MIDDLE0[14]
       FILE['Telefono_5'] = MIDDLE0[16]
       FILE['Telefono_6'] = MIDDLE0[18]

       #Eliminación de caracteres especiales del campo "Telefono_1"
       FILE.rename({'Numero':'Telefono_1'}, axis=1, inplace=True)
       MIDDLETEL1 = (FILE["Telefono_1"].str.split("", n = 18, expand = True)) 
       MIDDLETEL1.replace(['-'],[""], inplace = True)
       MIDDLETEL1.replace(['_'],[""], inplace = True)
       MIDDLETEL1.replace(['.'],[""], inplace = True)
       MIDDLETEL1.replace([','],[""], inplace = True)
       MIDDLETEL1.replace([';'],[""], inplace = True)
       MIDDLETEL1.replace([')'],[""], inplace = True)
       MIDDLETEL1.replace(['('],[""], inplace = True)
       MIDDLETEL1.replace([None],[""], inplace = True)
       FILE['Telefono_1'] = MIDDLETEL1[1] + MIDDLETEL1[2] + MIDDLETEL1[3] + MIDDLETEL1[4] + MIDDLETEL1[4] + MIDDLETEL1[5] + MIDDLETEL1[6] + MIDDLETEL1[7] + MIDDLETEL1[8] + MIDDLETEL1[9] + MIDDLETEL1[10] + MIDDLETEL1[11] + MIDDLETEL1[12] + MIDDLETEL1[13] 
       del MIDDLETEL1

       #Eliminación de caracteres especiales del campo "Telefono_2"
       MIDDLETEL1 = (FILE["Telefono_2"].str.split("", n = 18, expand = True)) 
       MIDDLETEL1.replace(['-'],[""], inplace = True)
       MIDDLETEL1.replace(['_'],[""], inplace = True)
       MIDDLETEL1.replace(['.'],[""], inplace = True)
       MIDDLETEL1.replace([','],[""], inplace = True)
       MIDDLETEL1.replace([';'],[""], inplace = True)
       MIDDLETEL1.replace([')'],[""], inplace = True)
       MIDDLETEL1.replace(['('],[""], inplace = True)
       MIDDLETEL1.replace([None],[""], inplace = True)
       FILE['Telefono_2'] = MIDDLETEL1[1] + MIDDLETEL1[2] + MIDDLETEL1[3] + MIDDLETEL1[4] + MIDDLETEL1[4] + MIDDLETEL1[5] + MIDDLETEL1[6] + MIDDLETEL1[7] + MIDDLETEL1[8] + MIDDLETEL1[9] + MIDDLETEL1[10] + MIDDLETEL1[11] + MIDDLETEL1[12] + MIDDLETEL1[13]  
       del MIDDLETEL1

       #Eliminación de caracteres especiales del campo "Telefono_3"
       MIDDLETEL1 = (FILE["Telefono_3"].str.split("", n = 18, expand = True)) 
       MIDDLETEL1.replace(['-'],[""], inplace = True)
       MIDDLETEL1.replace(['_'],[""], inplace = True)
       MIDDLETEL1.replace(['.'],[""], inplace = True)
       MIDDLETEL1.replace([','],[""], inplace = True)
       MIDDLETEL1.replace([';'],[""], inplace = True)
       MIDDLETEL1.replace([')'],[""], inplace = True)
       MIDDLETEL1.replace(['('],[""], inplace = True)
       MIDDLETEL1.replace([None],[""], inplace = True)
       FILE['Telefono_3'] = MIDDLETEL1[1] + MIDDLETEL1[2] + MIDDLETEL1[3] + MIDDLETEL1[4] + MIDDLETEL1[4] + MIDDLETEL1[5] + MIDDLETEL1[6] + MIDDLETEL1[7] + MIDDLETEL1[8] + MIDDLETEL1[9] + MIDDLETEL1[10] + MIDDLETEL1[11] + MIDDLETEL1[12] + MIDDLETEL1[13]  
       del MIDDLETEL1

       #Eliminación de caracteres especiales del campo "Telefono_4"
       MIDDLETEL1 = (FILE["Telefono_4"].str.split("", n = 18, expand = True)) 
       MIDDLETEL1.replace(['-'],[""], inplace = True)
       MIDDLETEL1.replace(['_'],[""], inplace = True)
       MIDDLETEL1.replace(['.'],[""], inplace = True)
       MIDDLETEL1.replace([','],[""], inplace = True)
       MIDDLETEL1.replace([';'],[""], inplace = True)
       MIDDLETEL1.replace([')'],[""], inplace = True)
       MIDDLETEL1.replace(['('],[""], inplace = True)
       MIDDLETEL1.replace([None],[""], inplace = True)
       FILE['Telefono_4'] = MIDDLETEL1[1] + MIDDLETEL1[2] + MIDDLETEL1[3] + MIDDLETEL1[4] + MIDDLETEL1[4] + MIDDLETEL1[5] + MIDDLETEL1[6] + MIDDLETEL1[7] + MIDDLETEL1[8] + MIDDLETEL1[9] + MIDDLETEL1[10] + MIDDLETEL1[11] + MIDDLETEL1[12] + MIDDLETEL1[13]  
       del MIDDLETEL1

       #Eliminación de caracteres especiales del campo "Telefono_5"
       MIDDLETEL1 = (FILE["Telefono_5"].str.split("", n = 18, expand = True)) 
       MIDDLETEL1.replace(['-'],[""], inplace = True)
       MIDDLETEL1.replace(['_'],[""], inplace = True)
       MIDDLETEL1.replace(['.'],[""], inplace = True)
       MIDDLETEL1.replace([','],[""], inplace = True)
       MIDDLETEL1.replace([';'],[""], inplace = True)
       MIDDLETEL1.replace([')'],[""], inplace = True)
       MIDDLETEL1.replace(['('],[""], inplace = True)
       MIDDLETEL1.replace([None],[""], inplace = True)
       FILE['Telefono_5'] = MIDDLETEL1[1] + MIDDLETEL1[2] + MIDDLETEL1[3] + MIDDLETEL1[4] + MIDDLETEL1[4] + MIDDLETEL1[5] + MIDDLETEL1[6] + MIDDLETEL1[7] + MIDDLETEL1[8] + MIDDLETEL1[9] + MIDDLETEL1[10] + MIDDLETEL1[11] + MIDDLETEL1[12] + MIDDLETEL1[13]  
       del MIDDLETEL1

       #Eliminación de caracteres especiales del campo "Telefono_6"
       MIDDLETEL1 = (FILE["Telefono_6"].str.split("", n = 18, expand = True)) 
       MIDDLETEL1.replace(['-'],[""], inplace = True)
       MIDDLETEL1.replace(['_'],[""], inplace = True)
       MIDDLETEL1.replace(['.'],[""], inplace = True)
       MIDDLETEL1.replace([','],[""], inplace = True)
       MIDDLETEL1.replace([';'],[""], inplace = True)
       MIDDLETEL1.replace([')'],[""], inplace = True)
       MIDDLETEL1.replace(['('],[""], inplace = True)
       MIDDLETEL1.replace([None],[""], inplace = True)
       FILE['Telefono_6'] = MIDDLETEL1[1] + MIDDLETEL1[2] + MIDDLETEL1[3] + MIDDLETEL1[4] + MIDDLETEL1[4] + MIDDLETEL1[5] + MIDDLETEL1[6] + MIDDLETEL1[7] + MIDDLETEL1[8] + MIDDLETEL1[9] + MIDDLETEL1[10] + MIDDLETEL1[11] + MIDDLETEL1[12] + MIDDLETEL1[13]  
       del MIDDLETEL1

def ID_TRANFORMATION(FILE):
    """Se pasa a STR, se eliminan ". , - ( ) None", se renombra el campo "NumeroDocumento" y se crea el campo vacio "Tipo_Documento"

    Args:
         Parte de un CSV enviado por el socio.
    """    
    # Transformación a STR
    FILE['NumeroDocumento'] = FILE['NumeroDocumento'].astype(str)
    # Split para iterar
    DOC = (FILE['NumeroDocumento'].str.split("", n = 12, expand = True)) 
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
    FILE['NumeroDocumento'] = DOC[1] + DOC[2] + DOC[3] + DOC[4] + DOC[5]+ DOC[6] + DOC[7] + DOC[8] + DOC[9] + DOC[10] + DOC[11] + DOC[12]  
    # Rename del campo NumeroDocumento
    FILE.rename({'NumeroDocumento':'Nro_Documento'}, axis=1, inplace=True)
    # Creación del Campo Tipo_Documento
    FILE['Tipo_Documento'] = ""

# Normalización Marca y Modelo

def PRODUCT_TRANSFORMATION(FILE):
    """Se concatena Marca y Modelo por que vienen en campos separadas, luego se cambia el nombre del campo.

    Args:
         Se parte de un CSV enviado por el socio.
    """    

    # Concatenación Marca y Model
    FILE['Modelo1'] = FILE['Marca'] + " " + FILE['Modelo']
    # Delete de Campos temporales
    del FILE['Marca']
    del FILE['Modelo']
    # Rename del campo Modelo
    FILE.rename({'Modelo1':'Modelo'}, axis=1, inplace=True)
    # Limpieza de campos temporales

# Creación DF Persona

def ID_DF(FILE):
    """Se crea un DF con los datos de tipo y número de documento, patente y modelo.

    Args:
         Se parte de un CSV enviado por el Socio

    Returns:
        _type_: DF con datos de tipo y número de documento, patente y modelo.
    """         

    MIDDLE_PERSONA = pd.DataFrame()
    MIDDLE_PERSONA['Tipo_Documento'] = FILE['Tipo_Documento']
    MIDDLE_PERSONA['Nro_Documento'] = FILE['Nro_Documento']
    MIDDLE_PERSONA['Patente'] = FILE['Patente']
    MIDDLE_PERSONA['Modelo'] = FILE['Modelo']

    return MIDDLE_PERSONA

# Creación DF Contacto Teléfono

def TELEFON_DF(FILE):
    """Se crea un DF vertical con todos los telefonos asociados a cada documento, repitiendo documentos en los casos en que el cliente tenga mas de un teléfono , se crea un indice 
       autoincremental. Se eliminan Documentos-Telefonos duplicados.

    Args:
          Se parte de un CSV enviado por el Socio y previamente normalizado.

    Returns:
            Devuelve un DF con los datos de tipo y número de documento, todos los teléfonos y una indice autoincremental.
    """        

    MIDDLE_TEL1 = pd.DataFrame()
    MIDDLE_TEL1['Telefono_1'] = FILE['Telefono_1']
    MIDDLE_TEL1['Tipo_Documento'] = FILE['Tipo_Documento']
    MIDDLE_TEL1['Nro_Documento'] = FILE['Nro_Documento'] 

    MIDDLE_TEL2 = pd.DataFrame()
    MIDDLE_TEL2['Telefono_2'] = FILE['Telefono_2']
    MIDDLE_TEL2['Tipo_Documento'] = FILE['Tipo_Documento']
    MIDDLE_TEL2['Nro_Documento'] = FILE['Nro_Documento'] 

    MIDDLE_TEL3 = pd.DataFrame()
    MIDDLE_TEL3['Telefono_3'] = FILE['Telefono_3']
    MIDDLE_TEL3['Tipo_Documento'] = FILE['Tipo_Documento']
    MIDDLE_TEL3['Nro_Documento'] = FILE['Nro_Documento'] 

    MIDDLE_TEL4 = pd.DataFrame()
    MIDDLE_TEL4['Telefono_4'] = FILE['Telefono_4']
    MIDDLE_TEL4['Tipo_Documento'] = FILE['Tipo_Documento']
    MIDDLE_TEL4['Nro_Documento'] = FILE['Nro_Documento'] 

    MIDDLE_TEL5 = pd.DataFrame()
    MIDDLE_TEL5['Telefono_5'] = FILE['Telefono_5']
    MIDDLE_TEL5['Tipo_Documento'] = FILE['Tipo_Documento']
    MIDDLE_TEL5['Nro_Documento'] = FILE['Nro_Documento']

    MIDDLE_TEL6 = pd.DataFrame()
    MIDDLE_TEL6['Telefono_6'] = FILE['Telefono_6']
    MIDDLE_TEL6['Tipo_Documento'] = FILE['Tipo_Documento']
    MIDDLE_TEL6['Nro_Documento'] = FILE['Nro_Documento']

    # Concatenación horizontal
    MIDDLE_CONTACTO = pd.concat([MIDDLE_TEL1, MIDDLE_TEL2, MIDDLE_TEL3, MIDDLE_TEL4, MIDDLE_TEL5, MIDDLE_TEL6], axis=1)

    # Se eliminan las columnas que quedan en 0
    MIDDLE_CONTACTO.replace(['0'],[""], inplace = True)

    # Reseteo del Indice para que sea incremental
    MIDDLE_CONTACTO = MIDDLE_CONTACTO.reset_index()
    del MIDDLE_CONTACTO['index']

    # Delete de duplicados
    MIDDLE_CONTACTO.loc[(MIDDLE_CONTACTO['Telefono_1'] == MIDDLE_CONTACTO['Telefono_2']), 'Telefono_1'] = ""
    MIDDLE_CONTACTO.loc[(MIDDLE_CONTACTO['Telefono_1'] == MIDDLE_CONTACTO['Telefono_3']), 'Telefono_1'] = ""
    MIDDLE_CONTACTO.loc[(MIDDLE_CONTACTO['Telefono_1'] == MIDDLE_CONTACTO['Telefono_4']), 'Telefono_1'] = ""
    MIDDLE_CONTACTO.loc[(MIDDLE_CONTACTO['Telefono_1'] == MIDDLE_CONTACTO['Telefono_5']), 'Telefono_1'] = ""
    MIDDLE_CONTACTO.loc[(MIDDLE_CONTACTO['Telefono_1'] == MIDDLE_CONTACTO['Telefono_6']), 'Telefono_1'] = ""

    MIDDLE_CONTACTO.loc[(MIDDLE_CONTACTO['Telefono_2'] == MIDDLE_CONTACTO['Telefono_3']), 'Telefono_2'] = ""
    MIDDLE_CONTACTO.loc[(MIDDLE_CONTACTO['Telefono_2'] == MIDDLE_CONTACTO['Telefono_4']), 'Telefono_2'] = ""
    MIDDLE_CONTACTO.loc[(MIDDLE_CONTACTO['Telefono_2'] == MIDDLE_CONTACTO['Telefono_5']), 'Telefono_2'] = ""
    MIDDLE_CONTACTO.loc[(MIDDLE_CONTACTO['Telefono_2'] == MIDDLE_CONTACTO['Telefono_6']), 'Telefono_2'] = ""

    MIDDLE_CONTACTO.loc[(MIDDLE_CONTACTO['Telefono_3'] == MIDDLE_CONTACTO['Telefono_4']), 'Telefono_3'] = ""
    MIDDLE_CONTACTO.loc[(MIDDLE_CONTACTO['Telefono_3'] == MIDDLE_CONTACTO['Telefono_5']), 'Telefono_3'] = ""
    MIDDLE_CONTACTO.loc[(MIDDLE_CONTACTO['Telefono_3'] == MIDDLE_CONTACTO['Telefono_6']), 'Telefono_3'] = ""

    MIDDLE_CONTACTO.loc[(MIDDLE_CONTACTO['Telefono_4'] == MIDDLE_CONTACTO['Telefono_5']), 'Telefono_4'] = ""
    MIDDLE_CONTACTO.loc[(MIDDLE_CONTACTO['Telefono_4'] == MIDDLE_CONTACTO['Telefono_6']), 'Telefono_4'] = ""

    MIDDLE_CONTACTO.loc[(MIDDLE_CONTACTO['Telefono_5'] == MIDDLE_CONTACTO['Telefono_6']), 'Telefono_5'] = ""

    # Creación de DF para concatenar verticalmente
    MIDDLE_TELA1 = pd.DataFrame()
    MIDDLE_TELA1['Telefono_1'] = MIDDLE_CONTACTO['Telefono_1']
    MIDDLE_TELA1['Tipo_Documento'] = FILE['Tipo_Documento']
    MIDDLE_TELA1['Nro_Documento'] = FILE['Nro_Documento']

    MIDDLE_TELA2 = pd.DataFrame()
    MIDDLE_TELA2['Telefono_1'] = MIDDLE_CONTACTO['Telefono_2']
    MIDDLE_TELA2['Tipo_Documento'] = FILE['Tipo_Documento']
    MIDDLE_TELA2['Nro_Documento'] = FILE['Nro_Documento']

    MIDDLE_TELA3 = pd.DataFrame()
    MIDDLE_TELA3['Telefono_1'] = MIDDLE_CONTACTO['Telefono_3']
    MIDDLE_TELA3['Tipo_Documento'] = FILE['Tipo_Documento']
    MIDDLE_TELA3['Nro_Documento'] = FILE['Nro_Documento']

    MIDDLE_TELA4 = pd.DataFrame()
    MIDDLE_TELA4['Telefono_1'] = MIDDLE_CONTACTO['Telefono_4']
    MIDDLE_TELA4['Tipo_Documento'] = FILE['Tipo_Documento']
    MIDDLE_TELA4['Nro_Documento'] = FILE['Nro_Documento']

    MIDDLE_TELA5 = pd.DataFrame()
    MIDDLE_TELA5['Telefono_1'] = MIDDLE_CONTACTO['Telefono_5']
    MIDDLE_TELA5['Tipo_Documento'] = FILE['Tipo_Documento']
    MIDDLE_TELA5['Nro_Documento'] = FILE['Nro_Documento']

    MIDDLE_TELA6 = pd.DataFrame()
    MIDDLE_TELA6['Telefono_1'] = MIDDLE_CONTACTO['Telefono_6']
    MIDDLE_TELA6['Tipo_Documento'] = FILE['Tipo_Documento']
    MIDDLE_TELA6['Nro_Documento'] = FILE['Nro_Documento']

    # Concatenación vertical
    MIDDLE_CONTACTO_TELEFONO = pd.concat([MIDDLE_TELA1, MIDDLE_TELA2, MIDDLE_TELA3, MIDDLE_TELA4, MIDDLE_TELA5, MIDDLE_TELA6], axis=0)

    return MIDDLE_CONTACTO_TELEFONO

# Creación DF Contacto Email

def EMAIL_DF(FILE):
    """Se crea un DF vertical con todos los emails de cada documento, repitiendo en forma vertical los documentos, se crea un indice 
       autoincremental. Se eliminan Documentos-Emails duplicados.

    Args:
         Se parte de un CSV enviado por el socio previamente normalizado.

    Returns:
            Se devuelve un DF con dato de tipo y número de documento, todos los emails disponibles y un indice autoincremental.
    """    
 
    MIDDLE_EMA1 = pd.DataFrame()
    MIDDLE_EMA1['Email_1'] = FILE['Email_1']
    MIDDLE_EMA1['Tipo_Documento'] = FILE['Tipo_Documento']
    MIDDLE_EMA1['Nro_Documento'] = FILE['Nro_Documento'] 

    MIDDLE_EMA2 = pd.DataFrame()
    MIDDLE_EMA2['Email_2'] = FILE['Email_2']
    MIDDLE_EMA2['Tipo_Documento'] = FILE['Tipo_Documento']
    MIDDLE_EMA2['Nro_Documento'] = FILE['Nro_Documento']

    MIDDLE_EMA3 = pd.DataFrame()
    MIDDLE_EMA3['Email_3'] = FILE['Email_3']
    MIDDLE_EMA3['Tipo_Documento'] = FILE['Tipo_Documento']
    MIDDLE_EMA3['Nro_Documento'] = FILE['Nro_Documento']

    # Concatenación horizontal
    MIDDLE_CONTACTO_EMAIL = pd.concat([MIDDLE_EMA1, MIDDLE_EMA2, MIDDLE_EMA3], axis=1)

    # Delete de duplicados
    MIDDLE_CONTACTO_EMAIL.loc[(MIDDLE_CONTACTO_EMAIL['Email_1'] == MIDDLE_CONTACTO_EMAIL['Email_2']), 'Email_2'] = ""
    MIDDLE_CONTACTO_EMAIL.loc[(MIDDLE_CONTACTO_EMAIL['Email_1'] == MIDDLE_CONTACTO_EMAIL['Email_3']), 'Email_3'] = ""
    MIDDLE_CONTACTO_EMAIL.loc[(MIDDLE_CONTACTO_EMAIL['Email_2'] == MIDDLE_CONTACTO_EMAIL['Email_3']), 'Email_3'] = ""

    # Creación de DF para concatenar verticalmente
    MIDDLE_EMAIL1 = pd.DataFrame()
    MIDDLE_EMAIL1['Email_1'] = MIDDLE_CONTACTO_EMAIL['Email_1']
    MIDDLE_EMAIL1['Tipo_Documento'] = FILE['Tipo_Documento']
    MIDDLE_EMAIL1['Nro_Documento'] = FILE['Nro_Documento']

    MIDDLE_EMAIL2 = pd.DataFrame()
    MIDDLE_EMAIL2['Email_1'] = MIDDLE_CONTACTO_EMAIL['Email_2']
    MIDDLE_EMAIL2['Tipo_Documento'] = FILE['Tipo_Documento']
    MIDDLE_EMAIL2['Nro_Documento'] = FILE['Nro_Documento']

    MIDDLE_EMAIL3 = pd.DataFrame()
    MIDDLE_EMAIL3['Email_1'] = MIDDLE_CONTACTO_EMAIL['Email_3']
    MIDDLE_EMAIL3['Tipo_Documento'] = FILE['Tipo_Documento']
    MIDDLE_EMAIL3['Nro_Documento'] = FILE['Nro_Documento']

    # Concatenación horizontal
    MIDDLE_CONTACTO_EMAIL = pd.concat([MIDDLE_EMAIL1, MIDDLE_EMAIL2, MIDDLE_EMAIL3], axis=0)

    # Reseteo del Indice para que sea incremental
    MIDDLE_CONTACTO_EMAIL = MIDDLE_CONTACTO_EMAIL.reset_index()
    del MIDDLE_CONTACTO_EMAIL['index']

    

# Ejecución

#Read
READ_CSV
#Validation
COLUMNS_NUMBER(FILE)
COLUMNS_NAME(FILE)
#Standardization
EMAILS_TRANSFORMATION(FILE)
TELEFONS_TRANFORMATION(FILE)
ID_TRANFORMATION(FILE)
PRODUCT_TRANSFORMATION(FILE)
#Output
ID_DF(FILE)
TELEFON_DF(FILE)
EMAIL_DF(FILE)