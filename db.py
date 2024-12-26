import   time
import   re
import   logging
import   pyodbc   # For python3 MSSQL
import   os 

import   settings   

_sql_conn=None

## ------------------------------------------------------------------
## ------------------------------------------------------------------

## DB Connect
    
async def dbConnect():

  server = os.getenv("SERVER")
  database = os.getenv("DATABASE")
  user = os.getenv("DB_USERNAME")
  password = os.getenv("DB_PASSWORD")

  ## get driver
  driver =  [item for item in pyodbc.drivers()]
  if len(driver) == 0:
      raise Exception("No driver found")

  driver = driver[-1]
  print(driver)    

  conn_string = (
      f"Driver={driver};Server={server};Database={database};UID={user};PWD={password};Encrypt=no"
  )

  try:
    print (conn_string)  
    _sql_conn = pyodbc.connect(conn_string)
  except Exception as e:  
    print ("Exception:::", e)
    logging.info("Exception:::")
    logging.info(e)
  return(_sql_conn)

## ------------------------------------------------------------------

## DB Close
   
async def dbClose(conn):
  conn.close()
  return(None)

## ------------------------------------------------------------------
## ------------------------------------------------------------------
  
## get IPs for devices not updated in X minutes
    
async def dbGetDeviceNotUpdated(dbConn, minutesBack):

  devicesNotUpdated = []
  devID = None
  adminIP = None

  if (dbConn is None):
    logging.info('dbUpdateDevices ERROR : DB handler invalid. Unable to access DB!!')
    return None
  if (minutesBack is None):
    logging.info('minutesBack invalid. !!')
    return None

  try:
    dbCursor = dbConn.cursor()

  except Exception as e:  
    print ("Exception:::", e)
    logging.info('dbUpdateDevices ERROR : DB handler invalid. Unable to access DB!!')
    logging.info("Exception:::")
    logging.info(e)
    return (None) 

  dbStr = '''   SELECT IIF( LEN(TRIM([adminIP])) > 6, [adminIP], [ip] ) AS IP 
  FROM [networkInfo].[dbo].[devices]  where DATEDIFF(minute, lastUpdate, getdate()) > {} '''.format( minutesBack )
  #print(dbStr)

  try:
    dbCursor.execute(dbStr)
      
    for row in dbCursor.fetchall():  ## if several hostnames, get last.... 
      if (row[0] is not None):
        devicesNotUpdated.append(row[0].strip())
      
  except Exception as e:  
    print("DB Operation failed...({})".format(dbStr))
    logging.info('DB ERROR : Merge error!!')
  	      
  dbConn.commit() 
  dbCursor.close()
  return( (devicesNotUpdated) )


## ------------------------------------------------------------------
  
  
async def updateNeighbors(dbConn, ip,  devHostname, neighborsList):

  if (dbConn is None):
    logging.info('dbUpdateDevices ERROR : DB handler invalid. Unable to access DB!!')
    return None
  if (devHostname is None):
    logging.info('devHostname invalid. !!')
    return None
  if (neighborsList is None):
    logging.info('neighborsList invalid. !!')
    return None

  try:
    insertCursor = dbConn.cursor()

  except Exception as e:  
    print ("Exception:::", e)
    logging.info('updateNeighbors ERROR : {}'.format(e))
    return (None) 

  for d in neighborsList:
    insertStr = '''MERGE [networkInfo].[dbo].[neighbors] WITH (SERIALIZABLE) AS T
      USING (VALUES ('{}', '{}', '{}', '{}', GETDATE())) AS 
      U ([hostname] , [ip] , [neighborHostname]  , [neighborIP]  , [lastUpdate])
      ON U.[hostname] = T.[hostname] AND U.[neighborHostname] = T.[neighborHostname]
      WHEN MATCHED THEN
      UPDATE 
      SET T.[ip] = U.[ip], T.[neighborIP] = U.[neighborIP], 
      T.[lastUpdate] = U.[lastUpdate]
      WHEN NOT MATCHED THEN
      INSERT ([hostname], [ip], [neighborHostname], [neighborIP], [lastUpdate])
      VALUES (U.[hostname], U.[ip], U.[neighborHostname], U.[neighborIP], U.[lastUpdate]);
      '''.format(devHostname, ip, d['name'], d['ip'] )
        
    if (settings.verbose):
      print(insertStr)

    try:
      insertCursor.execute(insertStr)
    except Exception as e:  
      print("DB OPERATION failed...({})".format(insertStr))
      logging.info('DB ERROR : Merge error!!')

  dbConn.commit() 
  insertCursor.close()
  return()

## ------------------------------------------------------------------
  

## ------------------------------------------------------------------
## ------------------------------------------------------------------
