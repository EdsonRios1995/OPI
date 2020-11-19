#!/usr/bin/env python
# coding: utf-8

#from watchdog.observers import Observer
#from watchdog.events import FileSystemEventHandler

import os
import re
import argparse
import pandas as pd
from itertools import product
import generateDB




rootDir = os.getcwd()
tamales = os.path.join(rootDir,"tamales_inc")
teinvento = os.path.join(rootDir,"teinvento_inc")
rawDataPath = os.path.join("crudo","generador","fuente")
processedDataPath = os.path.join("procesado","generador","fuente")
dateDic = {"Jan":"01", "Feb":"02", "Mar":"03", "Apr":"04",
           "May":"05", "Jun":"06", "Jul":"07", "Aug":"08",
           "Sep":"09", "Oct":"10", "Nov":"11", "Dec":"12"}
startDate = 0 #Esta variable puede ser útil para determinar fecha a partir de la cual quieres correr el código
ventasCols = ["year", "month", "country", "calorie_category", "flavor", "zone", "product_code", "product_name", "sales"]
factCols = ["year", "month", "sales", "id_region", "id_product"]
prodCols = ["id_product", "calorie_category", "product", "product_brand", "producer"]
regionCols = ["id_region", "country", "region"]





def loadFromDataPath(dataPath, cols):
    dataFrames = []
    for subdir, dirs, files in os.walk(dataPath):
        for file in files:
            if file.endswith(".csv"):
                path2File = subdir+"/"+file
                dataFrame = pd.read_csv(path2File, names = cols)
                dataFrames.append(dataFrame)
    df = pd.concat(dataFrames)    
    return(df)





def createMappingfile(tableName, realtimeFile = None, processedDataPath = processedDataPath):
    """Esta función devuelve region_table y product_table para los datos de tamales"""
    processedLocation = os.path.join(rootDir,processedDataPath)
    if not os.path.exists(processedLocation):
        os.makedirs(processedLocation)
    fileName = tableName+"_tamales_inc.csv"
    MappingFile = os.path.join(processedLocation,fileName)
    if os.path.isfile(MappingFile):
        if realtimeFile:
            mapping_df = pd.read_csv(path2TableFile)
        else:
            newtable_df = pd.read_csv(MappingFile)
            print("This mapping file already exists for history data.")
            return(newtable_df)
    else:
        if tableName =="region_table":
            newCols = ["country", "zone"]
            #We need a new index for region_id
            keepIndex = True
        elif tableName =="product_table":
            newCols = ["product_code","product_name","flavor","calorie_category"]
            keepIndex = False
        else:
            print("Only region_table and product_table are supported as mapping file.")
            return
        dataFrames = []
        for subdir, dirs, files in os.walk(tamales):
            for file in files:
                if file.endswith(".csv"):
                    path2File = subdir+"/"+file
                    dataFrame = pd.read_csv(path2File, names = ventasCols)
                    dataFrames.append(dataFrame)
        df = pd.concat(dataFrames)
        
        newtable_df = df[newCols].drop_duplicates().reset_index(drop=True) 
        if keepIndex:
            newtable_df.index.name = "id_region"
        #product_code has a one to many mapping issue which will be overwritten
        newtable_df.to_csv(MappingFile,index=keepIndex,sep=",")
    return(newtable_df)




def createData(newDataPath, df, clearData, datos, macro=False):
    """Toma DataFrame (df) y lo carga en el nuevo path (newDataPath) utilizando una etiqueta como macro. 
    Limpiará el cache si clearData es True"""
    if macro:
        filename = datos+"_"+macro+".csv"
    else:
        filename = datos+".csv"
    newFile = os.path.join(newDataPath,filename)
    #If file exists and we don't need to clear data
    if clearData:
            if not os.path.exists(newDataPath):
                os.makedirs(newDataPath)
            print("Limpiando y cargando datos para "+filename)
            df.to_csv(newFile,index=False,sep=",")
    else:
        if os.path.isfile(newFile) :
            print("Datos ya existen para "+filename)
            return
        else:
            if not os.path.exists(newDataPath):
                os.makedirs(newDataPath)
            print("Cargando datos para "+filename)
            df.to_csv(newFile,index=False,sep=",")




def load2PathYYYYMM(df, destPath, yearCol = "year", monthCol = "month", datos="tamales_inc", clearData = False, startDate = startDate):
    """
    Carga datos que se encuentran en el mismo directorio y los acomoda en datos crudos de acuerdo a su fecha.
    """
    #We get 6 digit pattern
    dfGrouped = df[[yearCol,monthCol]].groupby([yearCol,monthCol]).size().reset_index().drop(columns=[0])
    
    for index, rows in dfGrouped.iterrows():
        year = rows[yearCol]
        month = rows[monthCol]
        YYYYMM = str(year)+dateDic[month]
        #Aquí podemos cargar datos desde la fecha en que especifiquemos
        if startDate > int(YYYYMM):
            continue
        newDestPath = os.path.join(rootDir,destPath,YYYYMM)
        #We filter records having year and month values
        years = df[yearCol] == year
        months = df[monthCol] == month
        dataWithDate = df.loc[years & months]
        
        createData(newDestPath, dataWithDate, clearData, datos, YYYYMM)




def loadTamalRaw2Proc(rawDataPath = rawDataPath, processedDataPath=processedDataPath, clearData = False, startDate = startDate):
    """
    Loads processed data from Raw data. 
    """
    region_df = createMappingfile("region_table")
    product_df = createMappingfile("product_table")
    datos = "tamales_inc"
    #We use regex to make sure we only load tamales
    YYYYMMRegex = re.compile('({})_(\d\d\d\d\d\d)'.format(datos))
    path = os.path.join(rootDir,rawDataPath)
    for subdir, dirs, files in os.walk(path):
        for file in files:
            if not YYYYMMRegex.match(file):
                continue
            match = YYYYMMRegex.search(file)
            YYYYMM = match.group(2) 
            #Aquí podemos cargar datos desde la fecha en que especifiquemos
            if startDate > int(YYYYMM):
                continue
            newProcessedDataPath = os.path.join(rootDir,processedDataPath,YYYYMM)
            #We keep relevant data
            df1 = pd.read_csv(os.path.join(subdir,file))[["year", "month", "product_code", "zone", "sales"]]
            #We map region_id to zone
            df1['zone'] = df1['zone'].map(region_df.set_index('zone')['id_region'])
            df1Grouped = df1[["product_code","zone","sales"]].groupby(["product_code","zone"]).sum().reset_index()
            df1Grouped.rename(columns = {"product_code":"product","sales":"monthly_sales", "zone":"id_region"}, inplace = True)
            
            #Pendiente: arreglar error en 202003 cuando se incluyen nuevos valores
            if 202002 < int(YYYYMM):
                break
            #Empezando el año
            if file.endswith("01.csv"):
                df1Grouped["monthly_sales_acc"] = df1Grouped["monthly_sales"]
                df1Grouped["diff_prev_month_perc"] = None
                df1Grouped_prev = df1Grouped.copy()
            else:
                #Aseguramos que el se creen columnas con datos para producto nuevo
                df1Grouped["monthly_sales_acc"] = df1Grouped["monthly_sales"]
                df1Grouped["diff_prev_month_perc"] = None
                for index, rows in df1Grouped_prev.iterrows():
                        
                    
                    #Calculamos nuevo acumulado
                    df1Grouped.loc[(df1Grouped["product"]==rows["product"]) & (df1Grouped["id_region"]==rows["id_region"]),
                                   ["monthly_sales_acc"]] += rows["monthly_sales_acc"]
                    #Calculamos nuevo porcentaje
                    prevSales = df1Grouped.loc[(df1Grouped["product"]==rows["product"]) & (df1Grouped["id_region"]==rows["id_region"]),
                                               "monthly_sales"].item()
                    newPercentage = ((prevSales/rows["monthly_sales"] - 1)*100)
                    df1Grouped.loc[(df1Grouped["product"]==rows["product"]) & (df1Grouped["id_region"]==rows["id_region"]),
                               ["diff_prev_month_perc"]] = newPercentage

                df1Grouped_prev = df1Grouped.copy()
            
            createData(newProcessedDataPath, df1Grouped, clearData, datos, YYYYMM)





def placeRaw2path(folder, destPath, rootDir=rootDir, clearData = False):
    path2Folder = os.path.join(rootDir,folder)
    newDataPath = os.path.join(rootDir,destPath)
    for table in os.listdir(path2Folder):
        subDir = os.path.join(path2Folder,table)
        if table == "fact_table":
            #Acomodaremos estos datos por fecha uniendo todas las particiones
            dfData = loadFromDataPath(subDir,factCols)
            load2PathYYYYMM(dfData,destPath,datos=folder)
        elif table == "product_dim":
            #Acomodaremos estos datos en el directorio fuente uniendo todas las particiones
            dfData = loadFromDataPath(subDir,prodCols)
            dfData = dfData[["id_product", "calorie_category", "product", "producer"]]
            createData(newDataPath, dfData, clearData, "product_dim")
        else:
            #Acomodaremos estos datos en el directorio fuente uniendo todas las particiones
            dfData = loadFromDataPath(subDir,regionCols)
            createData(newDataPath, dfData, clearData, "region_dim")




def loadTamalesInc():    
    print("Cargando datos de Tamales Inc.")
    df = loadFromDataPath(tamales, ventasCols)
    print("Cargando datos crudos...")
    load2PathYYYYMM(df, rawDataPath)
    print("Cargando datos procesados...")
    loadTamalRaw2Proc()





def loadTeinventoInc():
    """No proceso estos datos, sólo los ubico en los dataPath mencionados, y los separo si tienen columna year y month"""
    print("Cargando datos de Teinvento Inc.")
    print("Cargando datos crudos...")
    placeRaw2path("teinvento_inc", rawDataPath)
    print("Cargando datos procesados...")
    placeRaw2path("teinvento_inc", processedDataPath)




def insertRowsTamales(con_tamales):
    generateDB.sqlInsertRegion(con_tamales, os.path.join(rootDir,processedDataPath,"region_table_tamales_inc.csv"))
    generateDB.sqlInsertProductTamales(con_tamales, os.path.join(rootDir,processedDataPath,'product_table_tamales_inc.csv'))
    datos = "tamales_inc"
    YYYYMMRegex = re.compile('({})_(\d\d\d\d\d\d)'.format(datos))
    path = os.path.join(rootDir,rawDataPath)
    for subdir, dirs, files in os.walk(path):
        for file in files:
            if YYYYMMRegex.match(file):
                match = YYYYMMRegex.search(file)
                YYYYMM = match.group(2) 
                newFile = os.path.join(rootDir,processedDataPath,file)
                generateDB.sqlInsertTamalesInc(con_tamales, newFile, YYYYMM)
def insertRowsTeinvento(con_teinvento):
    generateDB.sqlInsertRegion(con_teinvento, os.path.join(rootDir,processedDataPath,"region_dim.csv"))
    generateDB.sqlInsertProductTeinvento(con_teinvento, os.path.join(rootDir,processedDataPath,'product_dim.csv'))
    datos = "teinvento_inc"
    YYYYMMRegex = re.compile('({})_(\d\d\d\d\d\d)'.format(datos))
    path = os.path.join(rootDir,rawDataPath)
    for subdir, dirs, files in os.walk(path):
        for file in files:
            if YYYYMMRegex.match(file):
                match = YYYYMMRegex.search(file)
                YYYYMM = match.group(2) 
                newFile = os.path.join(rootDir,processedDataPath,file)
                generateDB.sqlInsertTeinventoInc(con_teinvento, newFile, YYYYMM)





if __name__ == "__main__":
    loadTamalesInc()
    loadTeinventoInc()
    con_tamales = generateDB.sql_connection('tamales_inc.db')
    con_teinvento = generateDB.sql_connection('teinvento_inc.db')
    
    insertRowsTamales(con_tamales)
    insertRowsTeinvento(con_teinvento)

    con_tamales.close()
    con_teinvento.close()

