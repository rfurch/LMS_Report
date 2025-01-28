import   re
import   asyncio
import   sys
import   pandas as pd
import   numpy as np
import   argparse
import   time
import   logging
import   collections 
import   inspect

import   util as util
import   db as db

import   settings

from     datetime import datetime
import   os
import   dotenv
import   pyodbc

_dbHandler = None

## ------------------------------------------------------------------
## ------------------------------------------------------------------

###  !!! NOTES !!!!
'''
  Initial report was extracted from L2 BUT  now is gathered FROM QMOS  (REPORT # 102 LMF HEAT SHEET)
  Then we join with L2 by Heat number  (ONE RECORD BY HEAT)

'''
## ------------------------------------------------------------------
## ------------------------------------------------------------------
## ------------------------------------------------------------------

async def completeAdditionsByHeat(dbConn, l2l3rep, matAdditions, heatNumber):

  try:
    additionList = ['AlWire', 'Al Wire','Argon',' BAUXITE',' CaSi', 'FeV', 'Chrome',' Coke',' Dolime',' FeSi',' Inj C',' Lime',' Low Sulfur Carbon',' MIX',' NaturalGas',' Natural Gas',' Nitrogen',' Oxygen',' SAF','SiMn', 'FeMn']
    additionList = [x.strip(' ') for x in additionList]
    #print (additionList)

    ## Create columns for well known elements in DF
    for index, element in enumerate(additionList):
      weightName = 'WEIGHT_' + element
      weightName = re.sub('\s+', '_', weightName) 
    
      lenghtName = 'LENGTH_' + element
      lenghtName = re.sub('\s+', '_', lenghtName) 

      ## add this material to specific row where HEat Number matches
      l2l3rep.loc[l2l3rep['HEAT_ID'].str.strip() == heatNumber.strip(), weightName] = 0
      l2l3rep.loc[l2l3rep['HEAT_ID'].str.strip() == heatNumber.strip(), lenghtName] = 0

    ## go through additions and add them to l2l3report (one line per heat)
    for index, row in matAdditions.iterrows():
      if (row['MATERIAL_ID'].strip() in additionList):
        if (settings.verbose > 2):
          print(row['MATERIAL_ID'], row['WEIGHT_ACT'], row['LENGTH_ACT'])
        
        weightName = 'WEIGHT_' + row['MATERIAL_ID']
        weightName = re.sub('\s+', '_', weightName) 
     
        lenghtName = 'LENGTH_' + row['MATERIAL_ID']
        lenghtName = re.sub('\s+', '_', lenghtName) 

        if (settings.verbose > 2):
          print(weightName)   
          print(lenghtName)

        ## add this material to specific row where HEat Number matches
        l2l3rep.loc[l2l3rep['HEAT_ID'].str.strip() == heatNumber.strip(), weightName] = row['WEIGHT_ACT']
        l2l3rep.loc[l2l3rep['HEAT_ID'].str.strip() == heatNumber.strip(), lenghtName] = row['LENGTH_ACT']

      else:
        print(f" ****** Warning: addition material *{row['MATERIAL_ID']}* NOT LISTED / INCLUDED")
        ## add this column with ZERO values to avoid mismatch in DB
         

  except Exception as e:  
    print(" !!!!!  completeAdditionsByHeat: Operation failed...({})".format(e))
    logging.info('Exception in completeAdditionsByHeat!!')

  return(l2l3rep)


## ------------------------------------------------------------------

async def completeChemicalByHeat(dbConn, report, l2_chem, heatNumber):

  try:
    chemMatList = ['Fe','C','Si','Mn','P','S','Cr','Mo','Ni','Al','As','B','Co','Cu','Nb','W','Pb','Sn','Sb','Ti','V','Bi','Zr','Se','Zn','Ce','Hg','Cd','Ta','Te','LecoC','LecoN','LecoS','LecoO','LQD','J1','J2','J3','J4','J5','J6','J7','J8','J9','J10','J12','J14','J16','J18','J20','J24','J28','J32','DI Value']
    chemMatList = [x.strip(' ') for x in chemMatList]
    
    if (settings.verbose > 2):
      print (chemMatList)

    ## Create columns for well known elements in DF
    for index, element in enumerate(chemMatList):
      elementName = 'TOTAL_' + element
      elementName = re.sub('\s+', '_', elementName) 

      ## add this material to specific row where HEat Number matches
      report.loc[report['HEAT_ID'].str.strip() == heatNumber.strip(), elementName] = 0

    ## go through additions and add them to l2l3report (one line per heat)
    for index, row in l2_chem.iterrows():
      if (row['ELEM_NAME'].strip() in chemMatList):
        if (settings.verbose > 2):
          print(row['ELEM_NAME'], row['ELEM_VALUE'])
        
        elementName = 'TOTAL_' + row['ELEM_NAME']
        elementName = re.sub('\s+', '_', elementName) 
     
        if (settings.verbose > 2):
          print(elementName)   

        ## add this material to specific row where HEat Number matches
        report.loc[report['HEAT_ID'].str.strip() == heatNumber.strip(), elementName] = row['ELEM_VALUE']

      else:
        print(f" ****** Warning: ChemAna material *{row['ELEM_NAME']}* NOT LISTED / INCLUDED")

  except Exception as e:  
    print(" !!!!!  completeChemicalByHeat: Operation failed...({})".format(e))
    logging.info('Exception in completeChemicalByHeat!!')

  return(report)

## ------------------------------------------------------------------

async def completeFreeOpenByHeat(dbConn, report, ladleTimesDF, heatNumber):

  try:

    report.loc[report['HEAT_ID'].str.strip() == heatNumber.strip(), 'QMOS_GRADE'] = ladleTimesDF['GRADE']
    report.loc[report['HEAT_ID'].str.strip() == heatNumber.strip(), 'QMOS_STIR_ON'] = ladleTimesDF['STIR ON']
    report.loc[report['HEAT_ID'].str.strip() == heatNumber.strip(), 'QMOS_STIR_OFF'] = ladleTimesDF['STIR OFF']
    report.loc[report['HEAT_ID'].str.strip() == heatNumber.strip(), 'QMOS_FREE_OPEN'] = ladleTimesDF['FREE OPEN']

  except Exception as e:  
    print(" !!!!!  completeFreeOpenByHeat: Operation failed...({})".format(e))
    logging.info('Exception in completeFreeOpenByHeat!!')

  return(report)

## ------------------------------------------------------------------

async def completeO2ByHeat(dbConn, report, heatNumber):

  try:

    o2df = await db.dbGetL2L3OxigenByHeat(dbConn, heatNumber)
    if len(o2df) > 0 :
      report.loc[report['HEAT_ID'].str.strip() == heatNumber.strip(), 'O2_VALUE'] = o2df['O2_VALUE']
      report.loc[report['HEAT_ID'].str.strip() == heatNumber.strip(), 'O2_UNITS'] = o2df['O2_UNITS']
      report.loc[report['HEAT_ID'].str.strip() == heatNumber.strip(), 'O2_AREA'] = o2df['O2_AREA']
    else:      
      report.loc[report['HEAT_ID'].str.strip() == heatNumber.strip(), 'O2_VALUE'] = 0
      report.loc[report['HEAT_ID'].str.strip() == heatNumber.strip(), 'O2_UNITS'] = '---'
      report.loc[report['HEAT_ID'].str.strip() == heatNumber.strip(), 'O2_AREA'] = '---'

  except Exception as e:  
    print(" !!!!!  completeO2ByHeat: Operation failed...({})".format(e))
    logging.info('Exception in completeO2ByHeat!!')

  return(report)

## ------------------------------------------------------------------

async def completeTempByHeat(dbConn, report, heatNumber):

  try:

    tempdf = await db.dbGetL2L3TemperatureByHeat(dbConn, heatNumber)

    print(f"  ----    {tempdf}   ----")

    if len(tempdf) > 0 :
      report.loc[report['HEAT_ID'].str.strip() == heatNumber.strip(), 'TEMP_VALUE'] = tempdf['TEMP_VALUE']
      report.loc[report['HEAT_ID'].str.strip() == heatNumber.strip(), 'TEMP_UNITS'] = tempdf['TEMP_UNITS']
      report.loc[report['HEAT_ID'].str.strip() == heatNumber.strip(), 'TEMP_AREA'] = tempdf['TEMP_AREA']
      report.loc[report['HEAT_ID'].str.strip() == heatNumber.strip(), 'TEMP_MEAS_TIME'] = tempdf['TEMP_MEAS_TIME']
    else:      
      report.loc[report['HEAT_ID'].str.strip() == heatNumber.strip(), 'TEMP_VALUE'] = 0
      report.loc[report['HEAT_ID'].str.strip() == heatNumber.strip(), 'TEMP_UNITS'] = '---'
      report.loc[report['HEAT_ID'].str.strip() == heatNumber.strip(), 'TEMP_AREA'] = '---'
      report.loc[report['HEAT_ID'].str.strip() == heatNumber.strip(), 'TEMP_MEAS_TIME'] = 0

  except Exception as e:  
    print(" !!!!!  completeO2ByHeat: Operation failed...({})".format(e))
    logging.info('Exception in completeO2ByHeat!!')

  return(report)

## ------------------------------------------------------------------

# compose one dataframe from information from l2 DB and oracle

async def getDBFromL2DBByHeat(dbConn, heatNumber):
    
  ## get Basic report From QMOS, Rep # 102  LMF Heat Sheet
  heatReport = await db.oracleGetLMFHeatSheet(heatNumber)
  heatReport.columns = heatReport.columns.str.upper()
  heatReport.rename(columns={"HEAT": "HEAT_ID"}, inplace=True)
  #l2l3rep = await db.dbGetL2L3RefReportByHeat (dbConn, heatNumber)
  if (settings.verbose > 1):
    print("-------  report # 102  LMF Heat Sheet -------")
    print(heatReport)
    print(heatReport.info())

  ## get stir and ladle times from ORACLE
  ladleTimesDF = await db.oracleGetStirLadleTimes(heatNumber)
  ladleTimesDF.columns = ladleTimesDF.columns.str.upper()
  ladleTimesDF.rename(columns={"HEAT_NO": "HEAT_ID"}, inplace=True)
  if (settings.verbose > 1):
    print("-------  report # 1704 Stir and ladle times from QMOS -------")
    print(ladleTimesDF)
    print(ladleTimesDF.info())

  ## get additions
  matAdditions = await db.dbGetMaterialAdditionByHeat(dbConn, heatNumber)
  if (settings.verbose > 1):
    print("-------  Merging info from Material additions to L2L3 Report Table -------")
    print(matAdditions)
    print(matAdditions.info())

  ## get L2 chem analisys 
  l2_chem = await  db.dbGetL2ChemResultByHeat (dbConn, heatNumber)
  if (settings.verbose > 2):
    print("-------  Merging info from Chem. analisys -------")
    print(l2_chem)
    print(l2_chem.info())

  ## group by element and get last analisys (it says first due to DB sort....) 
  l2_chem = l2_chem.groupby(['HEAT_ID',  'ELEM_TYPE']).first()
  l2_chem.drop(columns = ['MSG_COUNTER'], inplace=True)
  l2_chem = l2_chem.reset_index()

  if (settings.verbose > 1):
    print(l2_chem)
    print(l2_chem.info())

  # add O2 meassurment if available
  resultDF = await completeO2ByHeat(dbConn, heatReport, heatNumber)

  # add Temp. meassurment if available
  resultDF = await completeTempByHeat(dbConn, resultDF, heatNumber)

  # special 'pivot'  from additions into final report
  resultDF = await completeAdditionsByHeat(dbConn, resultDF, matAdditions, heatNumber)

  # special 'pivot'  from ChemAnalisys into final report
  resultDF = await completeChemicalByHeat(dbConn, resultDF, l2_chem, heatNumber)

  # add free open and stir times
  resultDF = await completeFreeOpenByHeat(dbConn, resultDF, ladleTimesDF, heatNumber)

  if (settings.verbose > 3):
    if (resultDF is not None):
      print(resultDF)  
      print(resultDF.info())
    else:
      print("**** result is NONE!!!!!")  
    
  ## send result to excel as plain sheet 
  #with pd.ExcelWriter('output.xlsx') as writer:
  #  resultDF.to_excel(writer)

  ## send result to CSV
  #resultDF.to_csv('output.csv')  

  return(resultDF)

## ------------------------------------------------------------------
## ------------------------------------------------------------------

async def main():

  lastNHeats = 0
  heatToProcess = 0
  heatList=[]
  deleteBeforeProcess = False

  parser = argparse.ArgumentParser(description='Traffic totalization.')
  parser.add_argument('-v', '--verbose', action='count', default=0, help='Enables a verbose mode [-v, -vv, -vvv]')
  parser.add_argument('-D', '--debug', action='count', default=0, help='Debug level [-D, -DD, -DDD]')
  parser.add_argument('-N', '--processHeatNumber', help='Process Heat Number N', action='store', required=False)
  parser.add_argument('-L', '--lastNHeats', help='Process Last N heats (default: process last HEAT)', action='store', required=False)
  parser.add_argument('-d', '--deleteBeforeProcess', help='Delete record (if exists) and then re - create ', action='store_true', required=False)

  try:
    args = vars(parser.parse_args())
  except:
    parser.print_help()
    sys.exit(0)

  if args['verbose'] != None:
    settings.verbose = int(args['verbose'])

  if args['debug'] != None:
    settings.debug = int(args['debug'])

  if args['lastNHeats'] != None:
    lastNHeats = int(args['lastNHeats']) 

  if args['processHeatNumber'] != None:
    heatToProcess = int(args['processHeatNumber']) 

  if args['deleteBeforeProcess'] != None:
    deleteBeforeProcess = (args['deleteBeforeProcess']) 

  os.makedirs("logs", exist_ok=True)
  logger = logging.getLogger()
  logging.basicConfig(
      filename=f"logs/{datetime.now().strftime('%Y-%m-%d')}.log",
      level=logging.INFO,
      format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
  )
  logger.info("Starting ETL process")
  logger.info("Getting connection and cursor")

  dotenv.load_dotenv()  
  print(f" process PID: {os.getpid()}")

  #oradf = await db.oracleGetLMFHeatSheet('%1100064818%')
  #oradf.rename(columns={"heat": "HEAT_ID"}, inplace=True)
  #print(oradf)
  #exit(0)

  _dbHandler = await db.dbConnect()
  if (_dbHandler is None):
    logging.info( sys.argv[0] + ' DB init error. Exiting....')
    exit(1)
  else:
    logging.info( sys.argv[0] + ' DB init OK! ')

  try: 
    # evaluate heats to process
    if heatToProcess > 0:   # process only one specific heat
      heatList.append(str(heatToProcess))
      print(heatList)

    else:  
      if lastNHeats > 1:   # process several heats
        lastHeatsDF = await db.dbGetLastheatNumbers(_dbHandler, lastNHeats)
        heatList = lastHeatsDF['HEAT_ID'].to_numpy()
        print(heatList)

    for n, heatNumber in enumerate(heatList):
      print (n, str(heatNumber))

      if deleteBeforeProcess:  # delete if exists and re - create from QMOS / L2 Values
        await db.dbDeleteHeatFromFinalReport(int(heatNumber))  ## delete heat and re - insert

      if n == 0:    # first iteration, assign to DF
        reportDF = await getDBFromL2DBByHeat(_dbHandler, str(heatNumber))
      else:  # just append new rows to DF
        df = await getDBFromL2DBByHeat(_dbHandler, str(heatNumber))
        reportDF = pd.concat([reportDF, df])
        #reportDF.append(df)

  except Exception as e:  
    print(f" !!!!!  main:  exception: {e}")
    print(reportDF.info(verbose=True))
    print(df.info(verbose=True))
    ##print(reportDF.columns.difference(df.columns))
    ##print(reportDF.compare(df) )
    #print( "Not in 1: ", set(reportDF.columns.values) - set(df.columns.values))
    #print( "Not in 2: ", set(df.columns.values) - set(reportDF.columns.values))


  reportDF['HEAT_ID_NUM'] = pd.to_numeric(reportDF['HEAT_ID'], errors='coerce').fillna(0).astype(np.int64)

  ## send result to excel as plain sheet 
  with pd.ExcelWriter('output.xlsx') as writer:
    reportDF.to_excel(writer)

  ## send result to CSV
  reportDF.to_csv('output.csv')  

  ## send to DB. set condition to delete record before processing
  deleteRecord = False
  if heatToProcess > 0:
    deleteRecord = True

  print("Sending DF to DB: ", await db.sendDataframe2DB(reportDF, 'LMSReport'))

  logging.info( sys.argv[0] + ' ends OK !')
  return() 

## ------------------------------------------------------------------

if __name__ == "__main__":
    import time
    s = time.perf_counter()
    asyncio.run(main())
    elapsed = time.perf_counter() - s
    print(f"{__file__} executed in {elapsed:0.2f} seconds.")

## ------------------------------------------------------------------
## ------------------------------------------------------------------

