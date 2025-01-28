import   time
import   re
import   logging
import   pyodbc   # For python3 MSSQL
import   os 
import   pandas as pd

import   settings   

from     sqlalchemy import create_engine
import   cx_Oracle


_sql_conn=None

## ------------------------------------------------------------------
## ------------------------------------------------------------------

# get report # 1704 Stir and ladle times from QMOS
async def oracleGetStirLadleTimes(heatNumber):
  
  #  brjtxsqmosdb01.bar.nucorsteel.local [10.14.2.102]
  ORA_HOST = os.getenv("ORA_HOST")
  ORA_PORT = os.getenv("ORA_PORT")
  ORA_SERVICE = os.getenv("ORA_SERVICE")
  ORA_USERNAME = os.getenv("ORA_USERNAME")
  ORA_PASSWORD = os.getenv("ORA_PASSWORD")

  engine = create_engine(
    f'oracle+oracledb://:@',
        thick_mode=False,
        connect_args={
            "user": ORA_USERNAME,
            "password": ORA_PASSWORD,
            "host": ORA_HOST, 
            "port": ORA_PORT,
            "service_name": ORA_SERVICE
    })

  try:

    dbStr = '''SELECT V_MS_SUMMARY.MLH_HEAT_NO "HEAT NO", BLG_GRADE_ID "GRADE",
      Case When max(decode(HEAT_EVENTS.MEV_EVENT_ID, 'AL018',HEAT_EVENTS.EVENT,'AL059',HEAT_EVENTS.EVENT,null)) is null
      Then  max(decode(HEAT_EVENTS.MEV_EVENT_ID, 'ML01',HEAT_EVENTS.EVENT,'ML26',HEAT_EVENTS.EVENT,null))
      ELSE max(decode(HEAT_EVENTS.MEV_EVENT_ID, 'AL018',HEAT_EVENTS.EVENT,'AL059',HEAT_EVENTS.EVENT,null)) END "STIR ON",
      CASE WHEN max(decode(HEAT_EVENTS.MEV_EVENT_ID, 'AL021',HEAT_EVENTS.EVENT,'AL062',HEAT_EVENTS.EVENT,null))  is null              -- Checks if stir off is null
      THEN  max(decode(HEAT_EVENTS.MEV_EVENT_ID, 'MC026',HEAT_EVENTS.EVENT,null))                                                 -- If stir off is null replaces it with Ladle arrival
      ELSE 
        CASE WHEN max(decode(HEAT_EVENTS.MEV_EVENT_ID, 'AL021',HEAT_EVENTS.EVENT,'AL062',HEAT_EVENTS.EVENT,null)) 
        <  max(decode(HEAT_EVENTS.MEV_EVENT_ID, 'AL018',HEAT_EVENTS.EVENT,'AL059',HEAT_EVENTS.EVENT,null))                      -- If stir ON is greater than stir off
        THEN max(decode(HEAT_EVENTS.MEV_EVENT_ID, 'MC026',HEAT_EVENTS.EVENT,null))                                              -- Replace Stir OFF with Ladle arrival time
        ELSE  max(decode(HEAT_EVENTS.MEV_EVENT_ID, 'AL021',HEAT_EVENTS.EVENT,'AL062',HEAT_EVENTS.EVENT,null))                   -- If Stir On is smaller than stir off, keeps stir off time as original
        END 
      END                                                                                                         "STIR OFF",
      max(decode(HEAT_EVENTS.MEV_EVENT_ID, 'MC026',HEAT_EVENTS.EVENT,null)) "LADLE ARRIVAL",
      TO_CHAR(MLH_CAST_DATE, 'MM-DD-YYYY HH24:MI:SS') "LADLE OPEN",
      CASE WHEN MHR_CC_FREEOPEN = '0' THEN 'FALSE' ELSE 'TRUE' END "FREE OPEN", MHR_CREW_ID "CREW"
      FROM  V_MS_SUMMARY,
      (SELECT MLH_HEAT_NO,MEV_EVENT_ID, TO_CHAR(MAX(MHV_DATE), 'MM-DD-YYYY HH24:MI:SS') "EVENT" FROM MELT_HEAT_EVENT
      WHERE MEV_EVENT_ID IN ('AL018','AL059','AL021','AL062','MC026','ML01','ML26')
      GROUP BY MLH_HEAT_NO,MEV_EVENT_ID
       ) HEAT_EVENTS
      WHERE V_MS_SUMMARY.MLH_HEAT_NO = HEAT_EVENTS.MLH_HEAT_NO
      AND V_MS_SUMMARY.MLH_CAST_DATE IS NOT NULL
      AND V_MS_SUMMARY.MLH_HEAT_NO like '{}'
      GROUP BY
      V_MS_SUMMARY.MLH_HEAT_NO, V_MS_SUMMARY.BLG_GRADE_ID, BLG_GRADE_ID, V_MS_SUMMARY.MLH_CAST_DATE, V_MS_SUMMARY.MHR_CC_FREEOPEN,
      V_MS_SUMMARY.MHR_CREW_ID, MLH_CAST_DATE, MHR_CC_FREEOPEN, MHR_CREW_ID, TO_CHAR(MLH_CAST_DATE, 'MM-DD-YYYY HH24:MI:SS'), CASE WHEN MHR_CC_FREEOPEN = '0' THEN 'FALSE' ELSE 'TRUE' END
      ORDER BY  V_MS_SUMMARY.MLH_CAST_DATE DESC'''.format(heatNumber)

    test_df = pd.read_sql_query(dbStr, engine)
      
  except Exception as e:  
    print("DB Operation failed...({})".format(dbStr))
    logging.info('DB ERROR : Merge error!!')
  	      
  #engine.close()
  return( test_df )

## ------------------------------------------------------------------

# get report # 102  LMF Heat Sheet

async def oracleGetLMFHeatSheet(heatNumber):
  
  #  brjtxsqmosdb01.bar.nucorsteel.local [10.14.2.102]
  ORA_HOST = os.getenv("ORA_HOST")
  ORA_PORT = os.getenv("ORA_PORT")
  ORA_SERVICE = os.getenv("ORA_SERVICE")
  ORA_USERNAME = os.getenv("ORA_USERNAME")
  ORA_PASSWORD = os.getenv("ORA_PASSWORD")

  engine = create_engine(
    f'oracle+oracledb://:@',
        thick_mode=False,
        connect_args={
            "user": ORA_USERNAME,
            "password": ORA_PASSWORD,
            "host": ORA_HOST, 
            "port": ORA_PORT,
            "service_name": ORA_SERVICE
    })

  try:

    dbStr = '''SELECT  main_table.*,  product_recipe.sop_practice_id 
       FROM
        (SELECT DISTINCT  mhr.mlh_heat_no AS heat,  mlh.mlh_tap_date as tap_date,
        mhr.mhr_crew_id AS crew, mhr.mhr_shift_id AS shift_id,  wol.wol_demand_no, 
        work_order.pcd_product_code, mlh.blg_grade_id, mlh.eqg_serial_no, material.lmf_kwh,
        round(material.lmf_kwh/decode(mhr.mhr_prime_weight/2000, 0, null, mhr.mhr_prime_weight/2000), 4) as kwh_ton,
        mhr.mhr_lmf_finaltemp AS tap_temp,  mhr.mhr_lmf_finalo2 AS tap_oxygen,
        mhr.mhr_lmf_finalc AS tap_carbon, mhr.mhr_tap_weight/2000 AS tap_weight,
        mhr.mhr_prime_weight/2000 AS cast_ton, mhr.mhr_lmf_startdate, mhr.mhr_lmf_enddate,
        round((mhr.mhr_lmf_enddate - mhr.mhr_lmf_startdate)*24*60*60, 3) AS treatment_time
       FROM 
        melt_heat_result mhr, melt_heat mlh, melt_heat_consumption mhc, work_order_link wol, work_order,
          (  SELECT mhc.mlh_heat_no AS heat, sum(mhc.mhp_qty) AS lmf_kwh
             FROM  melt_heat_consumption mhc, melt_heat_result mhr
             WHERE
              mhc.mtg_group_id = 'Energy'  AND mhc.wcr_work_center_id = 4 AND mhc.mlh_heat_no LIKE '{}'
             GROUP BY mhc.mlh_heat_no) material
       WHERE
        mlh.mlh_heat_no = mhr.mlh_heat_no(+)  AND mlh.mlh_heat_no = mhc.mlh_heat_no(+)
        AND mlh.mlh_heat_no = material.heat(+)  AND mlh.wor_order_no = wol.wol_supply_no
        AND work_order.wor_order_no = wol.wol_demand_no
        AND mlh.mlh_heat_no like '{}'
        ) main_table,     
        (
        SELECT 
          product_code_route.pcd_product_code, product_code_route.sop_practice_id, melt_heat.mlh_heat_no
        FROM 
          product_code_route, route_sequence, multiple_route, melt_heat, work_order
        WHERE 
          product_code_route.rot_route_id = route_sequence.rot_route_id AND
          product_code_route.ros_route_seq = route_sequence.ros_route_seq AND
          product_code_route.pcd_product_code = multiple_route.pcd_product_code AND
          product_code_route.rot_route_id = multiple_route.rot_route_id AND
          product_code_route.finishing_route_id = multiple_route.finishing_route_id AND
          multiple_route.mur_primary = 1 AND
          route_sequence.wcr_work_center_id = 4 AND
          melt_heat.wor_order_no = work_order.wor_order_no AND
          work_order.pcd_product_code = product_code_route.pcd_product_code 
        ) PRODUCT_RECIPE
      WHERE main_table.heat = product_recipe.mlh_heat_no(+)'''.format(heatNumber, heatNumber)

    test_df = pd.read_sql_query(dbStr, engine)
      
  except Exception as e:  
    print("DB Operation failed...({})".format(dbStr))
    logging.info('DB ERROR : Merge error!!')
  	      
  #engine.close()
  return( test_df )


## ------------------------------------------------------------------

## DB Connect
    
async def sendDataframe2DB(df, tableName):

  retValue = False
  HeatNo = 0
  server = os.getenv("DST_DB_SERVER")
  database = os.getenv("DST_DB_DATABASE")
  username = os.getenv("DST_DB_USERNAME")
  password = os.getenv("DST_DB_PASSWORD")

  ## get driver
  driver =  [item for item in pyodbc.drivers()]
  if len(driver) == 0:
      raise Exception("No driver found")

  driver = driver[-1]
  print(driver)    

  # Create SQLAlchemy engine for MSSQL
  engine = create_engine(f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}&Encrypt=no")

  # be sure index is correct, consecutive / incremental numbers
  df.reset_index(drop=True, inplace=True)
  #print (df)

  # Write DataFrame to MSSQL table
  
  for index, row in df.iterrows():

    try:

      HeatNo = (df.iloc[[index]])['HEAT_ID_NUM'].iloc[0]
      print(f" *** heat Number: {HeatNo}")
      #print(index)
      #print(df.iloc[[index]])
      #print(df.iloc[[index]][['HEAT_ID_NUM']])
      #print(type(df.iloc[[index]]))

      df.iloc[[index]].to_sql(tableName, con=engine, if_exists='append', index=False, chunksize=1)
      print(f"Table '{tableName}' created/updated successfully and data inserted for Head {HeatNo}.")

      retValue = True

    except Exception as e:
      print(f"An error occurred: {e} on 'to_sql'  for heat: {HeatNo}")

    #df.to_sql(tableName, con=engine, if_exists='replace', index=False)
    #df.to_sql(tableName, con=engine, if_exists='append', index=False, chunksize=1)
    #print(f"Table '{tableName}' created successfully and data inserted.")

  #retValue = True
  #engine.close()
  engine.dispose()

  #except Exception as e:
   # print(f"An error occurred: {e}")

  return(retValue) 

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

## delete ONE heat from Heat reports ( for total replace )
    
async def dbDeleteHeatFromFinalReport(heatNumber):

  result = False

  if heatNumber < 1000:
    print (f"dbDeleteHeatFromFinalReport: Invalid heat Number ({heatNumber})")
    logging.info(f"dbDeleteHeatFromFinalReport: Invalid heat Number ({heatNumber})")
    return(result)

  server = os.getenv("DST_DB_SERVER")
  database = os.getenv("DST_DB_DATABASE")
  username = os.getenv("DST_DB_USERNAME")
  password = os.getenv("DST_DB_PASSWORD")
    
  ## get driver
  driver =  [item for item in pyodbc.drivers()]
  if len(driver) == 0:
      raise Exception("No driver found")

  driver = driver[-1]
  print(driver)    

  conn_string = (
      f"Driver={driver};Server={server};Database={database};UID={username};PWD={password};Encrypt=no"
      )

  try:
    print (conn_string)  
    _sql_conn = pyodbc.connect(conn_string)
  except Exception as e:  
    print ("Exception:::", e)
    logging.info("Exception:::")
    logging.info(e)
    return(result)

  try:
    dbCursor = _sql_conn.cursor()
  except Exception as e:  
    print ("Exception:::", e)
    logging.info('dbDeleteHeatFromFinalReport ERROR : DB handler invalid. Unable to access DB!!')
    logging.info("Exception:::")
    logging.info(e)
    return (result) 

  dbStr = '''DELETE FROM  [ProcessReports].[dbo].[LMSReport] WHERE HEAT_ID_NUM = {}'''.format(heatNumber)

  try:
    dbCursor.execute(dbStr)
    result = True       
  except Exception as e:  
    print("DB Operation failed...({}). exception: {}".format(dbStr, e))
    logging.info('DB ERROR : Merge error!!')
  	      
  _sql_conn.commit() 
  dbCursor.close()
  _sql_conn.close() 

  return( result )

## ------------------------------------------------------------------
  
## get JOIN info from L2 L3 and REF Report (SMS L2)
    
async def dbGetL2L3RefReportByHeat(dbConn, heatNumber):

  if (dbConn is None):
    logging.info('dbGetL2L3RefReportByHeat ERROR : DB handler invalid. Unable to access DB!!')
    return None

  try:
    dbCursor = dbConn.cursor()

  except Exception as e:  
    print ("Exception:::", e)
    logging.info('dbGetL2L3RefReportByHeat ERROR : DB handler invalid. Unable to access DB!!')
    logging.info("Exception:::")
    logging.info(e)
    return (None) 

  dbStr = '''   
    SELECT TOP (50) 
	  l2l3lms.[HEAT_ID]  ,l2l3lms.[AREA_ID] ,l2l3lms.[HEAT_ORDER_ID], 
    l2l3lms.[HEAT_START], l2l3lms.[HEAT_STOP] ,[TOTAL_TREATMENT_TIME], 
    l2l3lms.[TOTAL_ENERGY], l2l3lms.[TOTAL_POWER_ON] ,l2l3lms.[TOTAL_ALLOYS] ,
    refrep.[PLANT_ID], 
	  refrep.[STAND_NO], refrep.[LADLE_ARRIVAL] ,refrep.[LADLE_DEPARTURE], 
    refrep.[INITIAL_METAL_WEIGHT], refrep.[INITIAL_SLAG_WEIGHT],
    refrep.[FINAL_METAL_WEIGHT], refrep.[FINAL_SLAG_WEIGHT], 
    refrep.[DEGASSING_TIME], refrep.[REQ_DEGASSING_TIME], refrep.[ACTUAL_TARGET_TEMP_MIN],
    refrep.[ACTUAL_TARGET_TEMP_AIM], refrep.[ACTUAL_TARGET_TEMP_MAX],
    refrep.[EST_TEMP_AT_STOP], refrep.[DEEP_VACUUM_TIME], refrep.[PUMP_DOWN_TIME],
    refrep.[LEAK_RATE_FILTER] ,refrep.[LEAK_RATE_FILTER_DATE],
    refrep.[LEAK_RATE_FILTER_AND_TANK] ,refrep.[LEAK_RATE_FILTER_AND_TANK_DATE]
	  FROM [api_qmos].[dbo].[L2_L3_HEAT_REPORT_LMS] l2l3lms
    LEFT JOIN  [smpdb].[dbo].[RPT_REF] refrep ON 
      refrep.[REPORT_NO] = l2l3lms.[REPORT_NO]
	    WHERE  HEAT_ID like '{}'
	    ORDER BY l2l3lms.[REPORT_NO] desc'''.format(heatNumber)


  try:

    l2l3rep = pd.read_sql(dbStr, dbConn)
      
  except Exception as e:  
    print("dbGetL2L3RefReportByHeat Operation failed...({})".format(dbStr))
    logging.info('dbGetL2L3RefReportByHeat ERROR : Merge error!!')
  	      
  dbConn.commit() 
  dbCursor.close()
  return( l2l3rep )


## ------------------------------------------------------------------
## get last N heats from CCM L2 report
    
async def dbGetLastheatNumbers(dbConn, nHeats):

  if (dbConn is None):
    logging.info('dbGetLastheatNumbers ERROR : DB handler invalid. Unable to access DB!!')
    return None

  try:
    dbCursor = dbConn.cursor()

  except Exception as e:  
    print ("Exception:::", e)
    logging.info('dbGetLastheatNumbers ERROR : DB handler invalid. Unable to access DB!!')
    logging.info("Exception:::")
    logging.info(e)
    return (None) 

  dbStr = '''   
      SELECT DISTINCT TOP ({})
      [AREA_ID],[HEAT_ID], [HEAT_ORDER_ID], [GRADE_ID]
      FROM [api_qmos].[dbo].[L2_L3_HEAT_REPORT_CCM]
      ORDER BY [HEAT_ID] DESC '''.format(nHeats)

  try:
    lastNHeats = pd.read_sql(dbStr, dbConn)
      
  except Exception as e:  
    print("dbGetLastheatNumbers failed...({})".format(dbStr))
    logging.info('dbGetLastheatNumbers ERROR!!')
  	      
  dbConn.commit() 
  dbCursor.close()
  return( lastNHeats )

## ------------------------------------------------------------------

## get JOIN info from L2 L3 and REF Report (SMS L2)
    
async def dbGetL2L3RefReport(dbConn):

  if (dbConn is None):
    logging.info('dbUpdateDevices ERROR : DB handler invalid. Unable to access DB!!')
    return None

  try:
    dbCursor = dbConn.cursor()

  except Exception as e:  
    print ("Exception:::", e)
    logging.info('dbUpdateDevices ERROR : DB handler invalid. Unable to access DB!!')
    logging.info("Exception:::")
    logging.info(e)
    return (None) 

  dbStr = '''   
    SELECT TOP (50) 
	  l2l3lms.[HEAT_ID]  ,l2l3lms.[AREA_ID] ,l2l3lms.[HEAT_ORDER_ID], 
    l2l3lms.[HEAT_START], l2l3lms.[HEAT_STOP] ,[TOTAL_TREATMENT_TIME], 
    l2l3lms.[TOTAL_ENERGY], l2l3lms.[TOTAL_POWER_ON] ,l2l3lms.[TOTAL_ALLOYS] ,
    refrep.[PLANT_ID], 
	  refrep.[STAND_NO], refrep.[LADLE_ARRIVAL] ,refrep.[LADLE_DEPARTURE], 
    refrep.[INITIAL_METAL_WEIGHT], refrep.[INITIAL_SLAG_WEIGHT],
    refrep.[FINAL_METAL_WEIGHT], refrep.[FINAL_SLAG_WEIGHT], 
    refrep.[DEGASSING_TIME], refrep.[REQ_DEGASSING_TIME], refrep.[ACTUAL_TARGET_TEMP_MIN],
    refrep.[ACTUAL_TARGET_TEMP_AIM], refrep.[ACTUAL_TARGET_TEMP_MAX],
    refrep.[EST_TEMP_AT_STOP], refrep.[DEEP_VACUUM_TIME], refrep.[PUMP_DOWN_TIME],
    refrep.[LEAK_RATE_FILTER] ,refrep.[LEAK_RATE_FILTER_DATE],
    refrep.[LEAK_RATE_FILTER_AND_TANK] ,refrep.[LEAK_RATE_FILTER_AND_TANK_DATE]
	  FROM [api_qmos].[dbo].[L2_L3_HEAT_REPORT_LMS] l2l3lms
    LEFT JOIN  [smpdb].[dbo].[RPT_REF] refrep ON 
      refrep.[REPORT_NO] = l2l3lms.[REPORT_NO]
	    WHERE refrep.[LADLE_ARRIVAL] > '2024-06-22 13:18:28.760'
      AND HEAT_ID > 1100000000
	    ORDER BY l2l3lms.[REPORT_NO] desc'''


  try:

    l2l3rep = pd.read_sql(dbStr, dbConn)
      
  except Exception as e:  
    print("DB Operation failed...({})".format(dbStr))
    logging.info('DB ERROR : Merge error!!')
  	      
  dbConn.commit() 
  dbCursor.close()
  return( l2l3rep )



## ------------------------------------------------------------------

## get  Oxigen  from L2 SMS
    
async def dbGetL2L3OxigenByHeat(dbConn, heatNumber):

  if (dbConn is None):
    logging.info('dbGetL2L3OxigenByHeat ERROR : DB handler invalid. Unable to access DB!!')
    return None

  try:
    dbCursor = dbConn.cursor()

  except Exception as e:  
    print ("Exception:::", e)
    logging.info('dbUpdateDevices ERROR : DB handler invalid. Unable to access DB!!')
    logging.info("Exception:::")
    logging.info(e)
    return (None) 

  dbStr = ''' 
    SELECT TOP (1) 
      msg.[MSG_CODE], msg.[MSG_NAME] ,msg.[MSG_CREATED] ,param.[PARAM_NAME]
      ,param.[PARAM_VALUE_NUMBER] AS O2_VALUE ,param.[PARAM_UNIT] AS O2_UNITS, res.[AREA_ID] AS O2_AREA ,res.[HEAT_ID]
      ,res.[MEAS_TIME], res.[MODE], res.[MEAS_CODE], res.[PARAM_COUNT]
    FROM [api_qmos].[dbo].[L2_L3_MESSAGE]  msg
    LEFT JOIN [api_qmos].[dbo].[L2_L3_PARAMETER] param   on msg.MSG_COUNTER = param.MSG_COUNTER
    LEFT JOIN [api_qmos].[dbo].[L2_L3_MEAS_RESULT] res on res.MSG_COUNTER = msg.MSG_COUNTER
    WHERE [PARAM_NAME] like '%O2%' AND res.[AREA_ID] LIKE '%LMS%' AND res.HEAT_ID like '{}' 
      ORDER BY res.[HEAT_ID] DESC, res.[MEAS_TIME] DESC'''.format(heatNumber)

  try:
    l2O2 = pd.read_sql(dbStr, dbConn)
      
  except Exception as e:  
    print("dbGetL2L3OxigenByHeat DB Operation failed...({})".format(dbStr))
    logging.info('dbGetL2L3OxigenByHeat DB ERROR : Merge error!!')
  	      
  dbConn.commit() 
  dbCursor.close()
  return( l2O2 )

## ------------------------------------------------------------------
  
## get  temperature from L2 SMS
    
async def dbGetL2L3TemperatureByHeat(dbConn, heatNumber):

  if (dbConn is None):
    logging.info('dbGetL2L3TemperatureByHeat ERROR : DB handler invalid. Unable to access DB!!')
    return None

  try:
    dbCursor = dbConn.cursor()

  except Exception as e:  
    print ("Exception:::", e)
    logging.info('dbUpdateDevices ERROR : DB handler invalid. Unable to access DB!!')
    logging.info("Exception:::")
    logging.info(e)
    return (None) 

  dbStr = ''' 
  SELECT TOP (1) 
	  msg.[MSG_COUNTER]  ,msg.[MSG_CREATED] ,param.[PARAM_NAME]
    ,param.[PARAM_VALUE_NUMBER] AS TEMP_VALUE ,param.[PARAM_UNIT] AS TEMP_UNITS
	  ,res.[MSG_COUNTER] , res.[AREA_ID] AS TEMP_AREA ,res.[HEAT_ID]
    ,res.[MEAS_TIME] AS TEMP_MEAS_TIME ,res.[MODE] ,res.[MEAS_CODE] ,res.[PARAM_COUNT]
  FROM [api_qmos].[dbo].[L2_L3_MESSAGE]  msg
    LEFT JOIN [api_qmos].[dbo].[L2_L3_PARAMETER] param   on msg.MSG_COUNTER = param.MSG_COUNTER
    LEFT JOIN [api_qmos].[dbo].[L2_L3_MEAS_RESULT] res on res.MSG_COUNTER = msg.MSG_COUNTER
  WHERE [PARAM_NAME] like 'TEMP' AND res.AREA_ID like 'LMS%' AND res.HEAT_ID like '{}'  
  ORDER BY res.[HEAT_ID] DESC  , res.[MEAS_TIME] DESC'''.format(heatNumber)

  try:
    l2O2 = pd.read_sql(dbStr, dbConn)
      
  except Exception as e:  
    print("dbGetL2L3TemperatureByHeat DB Operation failed...({})".format(dbStr))
    logging.info('dbGetL2L3TemperatureByHeat DB ERROR : Merge error!!')
  	      
  dbConn.commit() 
  dbCursor.close()
  return( l2O2 )

## ------------------------------------------------------------------
  
## get  Chemical An. from L2 SMS
    
async def dbGetL2ChemResultByHeat(dbConn, heatNumber):

  if (dbConn is None):
    logging.info('dbUpdateDevices ERROR : DB handler invalid. Unable to access DB!!')
    return None

  try:
    dbCursor = dbConn.cursor()

  except Exception as e:  
    print ("Exception:::", e)
    logging.info('dbUpdateDevices ERROR : DB handler invalid. Unable to access DB!!')
    logging.info("Exception:::")
    logging.info(e)
    return (None) 

  dbStr = '''   
    SELECT TOP (5000) 
      result.[MSG_COUNTER], result.[AREA_ID], result.[HEAT_ID],
      --result.[SAMPLE_ID], 
      --result.[TEST_TYPE] ,
      result.[SAMPLE_TIME],
      result.[ANALYSIS_TIME], 
      --result.[BAD_SAMPLE], result.[IS_FINAL],
      result.[ELEM_COUNT], element.[MSG_COUNTER], element.[ELEM_TYPE],
      element.[ELEM_NAME] ,element.[ELEM_VALUE] 
      --, element.[ELEM_UNIT]
  FROM [api_qmos].[dbo].[L2_L3_CHEM_RESULT] result
  LEFT JOIN [api_qmos].[dbo].[L2_L3_CHEM_RESULT_ELEMENT] element
  ON result.MSG_COUNTER = element.MSG_COUNTER
  WHERE AREA_ID LIKE 'LMS%' AND BAD_SAMPLE=0 AND IS_FINAL=1
  AND result.[HEAT_ID] LIKE '{}'
  ORDER BY result.MSG_COUNTER DESC'''.format(heatNumber)

  try:

    l2Chem = pd.read_sql(dbStr, dbConn)
      
  except Exception as e:  
    print("DB Operation failed...({})".format(dbStr))
    logging.info('DB ERROR : Merge error!!')
  	      
  dbConn.commit() 
  dbCursor.close()
  return( l2Chem )

## ------------------------------------------------------------------
  
## get  Chemical An. from L2 SMS
    
async def dbGetL2ChemResult(dbConn):

  if (dbConn is None):
    logging.info('dbUpdateDevices ERROR : DB handler invalid. Unable to access DB!!')
    return None

  try:
    dbCursor = dbConn.cursor()

  except Exception as e:  
    print ("Exception:::", e)
    logging.info('dbUpdateDevices ERROR : DB handler invalid. Unable to access DB!!')
    logging.info("Exception:::")
    logging.info(e)
    return (None) 

  dbStr = '''   
    SELECT TOP (5000) 
      result.[MSG_COUNTER], result.[AREA_ID], result.[HEAT_ID],
      --result.[SAMPLE_ID], 
      --result.[TEST_TYPE] ,
      result.[SAMPLE_TIME],
      result.[ANALYSIS_TIME], 
      --result.[BAD_SAMPLE], result.[IS_FINAL],
      result.[ELEM_COUNT], element.[MSG_COUNTER], element.[ELEM_TYPE],
      element.[ELEM_NAME] ,element.[ELEM_VALUE] 
      --, element.[ELEM_UNIT]
  FROM [api_qmos].[dbo].[L2_L3_CHEM_RESULT] result
  LEFT JOIN [api_qmos].[dbo].[L2_L3_CHEM_RESULT_ELEMENT] element
  ON result.MSG_COUNTER = element.MSG_COUNTER
  WHERE AREA_ID LIKE 'LMS%' AND BAD_SAMPLE=0 AND IS_FINAL=1
  ORDER BY result.MSG_COUNTER DESC'''

  try:

    l2Chem = pd.read_sql(dbStr, dbConn)
      
  except Exception as e:  
    print("DB Operation failed...({})".format(dbStr))
    logging.info('DB ERROR : Merge error!!')
  	      
  dbConn.commit() 
  dbCursor.close()
  return( l2Chem )


## ------------------------------------------------------------------

## get  Material Addition
    
async def dbGetMaterialAddition(dbConn):

  if (dbConn is None):
    logging.info('dbUpdateDevices ERROR : DB handler invalid. Unable to access DB!!')
    return None

  try:
    dbCursor = dbConn.cursor()

  except Exception as e:  
    print ("Exception:::", e)
    logging.info('dbUpdateDevices ERROR : DB handler invalid. Unable to access DB!!')
    logging.info("Exception:::")
    logging.info(e)
    return (None) 

  dbStr = '''   
    SELECT TOP (3000) 
      ---addi.[MSG_COUNTER]
      addi.[ADDITION_NO]
      --,addi.[REPORT_NO]
      ,addi.[AREA_ID]
      ,addi.[HEAT_ID]
      --,addi.[HEAT_ORDER_ID]
      ,addi.[ADDITION_TIME]
	    ---,addimat.[MSG_COUNTER]
      ,addimat.[ADDITION_NO] as [MATERIAL_NO]
      ,addimat.[MATERIAL_ID]
      --, CONCAT(addimat.[ADDITION_NO], '-', addimat.[MATERIAL_ID]) as MATERIAL
      --,addimat.[HEAT_ID]
      --,addimat.[HEAT_ORDER_ID]
      ,addimat.[WEIGHT_ACT]
      ,addimat.[LENGTH_ACT]
    FROM [api_qmos].[dbo].[L2_L3_HEAT_ADDITION] addi
    LEFT JOIN [api_qmos].[dbo].[L2_L3_HEAT_ADDITION_MATERIAL] addimat
    ON addi.HEAT_ID = addimat.HEAT_ID AND addi.MSG_COUNTER = addimat.MSG_COUNTER
    WHERE addi.AREA_ID LIKE 'LMS%' 
    --AND addi.HEAT_ID like '1100064702'
    ORDER  BY addi.HEAT_ID DESC, addi.[ADDITION_NO] DESC, addimat.[ADDITION_NO] DESC'''

  try:
    matAdd = pd.read_sql(dbStr, dbConn)      
  except Exception as e:  
    print("DB Operation failed...({})".format(dbStr))
    logging.info('DB ERROR : Merge error!!')
  	      
  dbConn.commit() 
  dbCursor.close()
  return( matAdd )

## ------------------------------------------------------------------

## get  Material Addition
    
async def dbGetMaterialAdditionByHeat(dbConn,heatNumber):

  if (dbConn is None):
    logging.info('dbUpdateDevices ERROR : DB handler invalid. Unable to access DB!!')
    return None

  try:
    dbCursor = dbConn.cursor()

  except Exception as e:  
    print ("Exception:::", e)
    logging.info('dbUpdateDevices ERROR : DB handler invalid. Unable to access DB!!')
    logging.info("Exception:::")
    logging.info(e)
    return (None) 

  dbStr = '''   
    SELECT TOP (1000) 
      addi.[AREA_ID] ,addi.[HEAT_ID]
      ,MIN(addi.[ADDITION_TIME]) AS MIN_ADD_TIME
      ,MAX(addi.[ADDITION_TIME]) AS MAX_ADD_TIME
      ,addimat.[MATERIAL_ID]
      ,SUM(addimat.[WEIGHT_ACT]) AS [WEIGHT_ACT]
      ,SUM(addimat.[LENGTH_ACT]) AS [LENGTH_ACT]
  FROM [api_qmos].[dbo].[L2_L3_HEAT_ADDITION] addi
  LEFT JOIN [api_qmos].[dbo].[L2_L3_HEAT_ADDITION_MATERIAL] addimat
  ON addi.HEAT_ID = addimat.HEAT_ID 
  WHERE addi.AREA_ID LIKE 'LMS%' 
  AND addimat.HEAT_ID like '{}'
  GROUP BY addi.[AREA_ID], addi.[HEAT_ID], addimat.[MATERIAL_ID]'''.format(heatNumber)

  try:
    matAdd = pd.read_sql(dbStr, dbConn)      
  except Exception as e:  
    print("DB Operation failed...({})".format(dbStr))
    logging.info('DB ERROR : Merge error!!')
  	      
  dbConn.commit() 
  dbCursor.close()
  return( matAdd )


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
