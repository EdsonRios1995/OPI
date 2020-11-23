import sqlite3
import csv
import os
from sqlite3 import Error

def sql_connection(db_name):
  try:
    if db_name == 'tamales_inc.db':
      con = sqlite3.connect(db_name)
      print("Connection is established: Database tamales created")
      sqlCreateTamalesIncTables(con)
      return con
    elif db_name == 'teinvento_inc.db':
      con = sqlite3.connect(db_name)
      print("Connection is established: Database teinvento created")
      sqlCreateTeinventoIncTables(con)
      return con
  except Error:
    print(Error)

def sqlCreateTamalesIncTables(con):
  cursorObj = con.cursor()
  cursorObj.execute("CREATE TABLE if not exists region_dim(id_region integer PRIMARY KEY, country text, region text)")
  cursorObj.execute("CREATE TABLE if not exists product_dim_tamales(id_product text PRIMARY KEY, calorie_category text, product_name text, flavor text)")
  cursorObj.execute("CREATE TABLE if not exists tamales_inc(year integer, month text, sales numeric, monthly_sales_acc numeric, diff_prev_month_perc numeric, id_product_FK text, id_region_FK integer, FOREIGN KEY(id_product_FK) REFERENCES product_dim_tamales(id_product), FOREIGN KEY(id_region_FK) REFERENCES region_dim(id_region))")
  con.commit()

def sqlCreateTeinventoIncTables(con):
  cursorObj = con.cursor()
  cursorObj.execute("CREATE TABLE if not exists region_dim(id_region integer PRIMARY KEY, country text, region text)")
  cursorObj.execute("CREATE TABLE if not exists product_dim_teinvento(id_product integer PRIMARY KEY, calorie_category text, product_name text, producer text)")
  cursorObj.execute("CREATE TABLE if not exists teinvento_inc(year integer, month text, sales numeric, id_product_FK integer, id_region_FK integer, FOREIGN KEY(id_product_FK) REFERENCES product_dim_teinvento(id_product), FOREIGN KEY(id_region_FK) REFERENCES region_dim(id_region))")
  con.commit()

def csv2Array(file,YYYYMM = None):
  if os.path.exists(file):
    with open(file, newline='') as f:
      try:
        next(f)
        entities = list(csv.reader(f))
        
        if YYYYMM:
          for entity in entities:
            entity.insert(0,YYYYMM[4:])
            entity.insert(0,YYYYMM[:4])
        return entities
      except:
        print('An error has occured')
def csv2ArrayTeinvento(file,YYYYMM = None):
  if os.path.exists(file):
    with open(file, newline='') as f:
      try:
        next(f)
        entities = list(csv.reader(f))
        
        if YYYYMM:
          for entity in entities:
            entity[1] = YYYYMM[4:]
        return entities
      except:
        print('An error has occured')
def sqlInsertRegion(con, path2data):
  try:
    entities = csv2Array(path2data)
    if entities:
      cursorObj = con.cursor()
      cursorObj.executemany('INSERT INTO region_dim(id_region, country, region) VALUES(?, ?, ?)', entities)
      con.commit()
      print('Data inserted into region_dim')
    else:
      con.close()
      print('ERROR trying to insert, could not find file', path2data)
    return
  except Error:
    print(Error)

def sqlInsertProductTamales(con, path2data):
  try:
    entities = csv2Array(path2data)
    if entities:
      cursorObj = con.cursor()
      cursorObj.executemany('INSERT INTO product_dim_tamales(id_product, product_name, flavor, calorie_category) VALUES(?, ?, ?, ?)', entities)
      con.commit()
      print('Data inserted into product_dim_tamales table')
    else:
      con.close()
      print('ERROR trying to insert, could not find file', path2data)
    return
  except Error:
    print(Error)

def sqlInsertTamalesInc(con, path2data, YYYYMM):
  try:
    entities = csv2Array(path2data,YYYYMM)
    if entities:
      cursorObj = con.cursor()
      cursorObj.executemany('INSERT INTO tamales_inc(year, month, id_product_FK, id_region_FK, sales, monthly_sales_acc, diff_prev_month_perc) VALUES(?, ?, ?, ?, ?, ?, ?)', entities)
      con.commit()
      print('Data inserted into tamales_inc table')
    else:
      con.close()
      print('ERROR trying to insert, could not find file', path2data)
    return
  except Error:
    print(Error)

def sqlInsertProductTeinvento(con, path2data):
  try:
    entities = csv2Array(path2data)
    if entities:
      cursorObj = con.cursor()

      cursorObj.executemany('INSERT INTO product_dim_teinvento(id_product, calorie_category, product_name, producer) VALUES(?, ?, ?, ?)', entities)
      con.commit()
      print('Data inserted into product_dim_teinvento table')
    else:
      con.close()
      print('ERROR trying to insert, could not find file', path2data)
    return
  except Error:
    print(Error)

def sqlInsertTeinventoInc(con, path2data,YYYYMM):
  try:
    entities = csv2ArrayTeinvento(path2data,YYYYMM)
    if entities:
      cursorObj = con.cursor()
      cursorObj.executemany('INSERT INTO teinvento_inc(year, month, sales, id_region_FK, id_product_FK) VALUES(?, ?, ?, ?, ?)', entities)
      con.commit()
      print('Data inserted into teinvento_inc table')
    else:
      con.close()
      print('ERROR trying to insert, could not find file', path2data)
    return
  except Error:
    print(Error)

