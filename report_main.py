import   re
import   asyncio
import   sys
import   pandas as pd
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


  
## ------------------------------------------------------------------
## ------------------------------------------------------------------


  
## ------------------------------------------------------------------
## ------------------------------------------------------------------

async def main():

  parser = argparse.ArgumentParser(description='Traffic totalization.')
  #parser.add_argument('-i', '--ip', help='IP address of switch to interrogate', required=True)
  #parser.add_argument('-g', '--getInfo', nargs='?', help='Get MAC / ARp / INTERFACE Info from switch', default=False)
  #parser.add_argument('-d', '--depth', help='Depth to go 0: this device only (how many levels or jumps through switches...[200]', required=False)
  #parser.add_argument('-n', '--noDB', nargs='?', default=False, help='NO DB connection, for debugging only', required=False)
  parser.add_argument('-v', '--verbose', action='count', default=0, help='Enables a verbose mode [-v, -vv, -vvv]')
  parser.add_argument('-D', '--debug', action='count', default=0, help='Debug level [-D, -DD, -DDD]')

  try:
    args = vars(parser.parse_args())
  except:
    parser.print_help()
    sys.exit(0)

  #if args['ip']:
  #  ip = args['ip']  
  #if args['depth']:
  #  depth = int(args['depth'])

  if args['verbose'] != None:
    settings.verbose = int(args['verbose'])

  if args['debug'] != None:
    settings.debug = int(args['debug'])

  ##if args['getInfo'] != False:
  ##  getInfo = True



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

  _dbHandler = await db.dbConnect()
  if (_dbHandler is None):
    logging.info( sys.argv[0] + ' DB init error. Exiting....')
    exit(1)
  else:
    logging.info( sys.argv[0] + ' DB init OK! ')


  
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

