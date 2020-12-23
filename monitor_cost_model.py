from decimal import Decimal
import requests
import json
import time
import sys
from config import config
import psycopg2
from datetime import datetime
import pytz
from decimal import Decimal

API = "https://api.thegraph.com/subgraphs/name/graphprotocol/graph-network-mainnet"


def openConnection():
    params = config()
    print('Connecting to the PostgreSQL database...')
    return psycopg2.connect(**params)


def create_table():
    conn = None
    try:
        conn = openConnection()
        cursor = conn.cursor()
        cursor.execute(
            '''Create table if not exists delegate_reward(
                    id SERIAL PRIMARY KEY,
                    timecollect bigint,
                    shareamount bigint,
                    reward bigint,
                    delegatewallet text)''')
        cursor.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


def insert_reward(timeCollect,shareAmount, reward, delegatewallet):
    conn = None
    try:
        conn = openConnection()
        cursor = conn.cursor()
        sqlInsert = f'''insert into delegate_reward("timecollect","reward","delegatewallet","shareamount") 
              values('{timeCollect}','{reward}','{delegatewallet}','{shareAmount}')'''
        cursor.execute(sqlInsert)
        cursor.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


def get_reward(indexer):
    query = '''
        {
          indexer(id: "%s") {
            delegators{
              delegator{
                id
                stakes{
                shareAmount
                personalExchangeRate
                indexer{
                  id
                  delegationExchangeRate
                }
              }
              totalRealizedRewards
              }
            }
          }
        }
    ''' % indexer
    result = requests.post(API, json={'query': query})
    resultObj = json.loads(result.text)
    return resultObj["data"]["indexer"]["delegators"]


def start(indexerWallet):
    create_table()
    # print("Create table success !")
    delegators = get_reward(indexerWallet)
    for delegator in delegators:
        delegator = delegator["delegator"]
        wallet = delegator["id"]
        stakes = delegator["stakes"]
        for stake in stakes:
            indexer = stake["indexer"]["id"]
            if indexer == indexerWallet:
                delegationExchangeRate = Decimal(stake["indexer"]["delegationExchangeRate"])
                personalExchangeRate = Decimal(stake["personalExchangeRate"])
                shareAmount = Decimal(stake["shareAmount"])
                if shareAmount > 0:
                    unrealizedReturn = (delegationExchangeRate / personalExchangeRate - 1) * shareAmount
                    if unrealizedReturn > 1:
                        unrealizedReturn = round(unrealizedReturn / 1000000000000000000)
                    else:
                        unrealizedReturn = 0
                    currentTime = round(time.time())
                    shareAmount = round(shareAmount / 1000000000000000000)
                    insert_reward(currentTime,shareAmount, unrealizedReturn, wallet)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        wallet = sys.argv[1]
        print("=======>  START Tracking Delegators reward for indexer  = %s <=======\n" % (wallet))
        start(wallet)
    else:
        print("ERROR : You must have 2 parameters (wallet and interval) in the command.")
