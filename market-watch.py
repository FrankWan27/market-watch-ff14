import requests
import asyncio
from collections import Counter
import sqlite3 as sl

#TODO: 99x quanitity, trade volume, cheapest world
WORLD = 'Cactuar'
DATACENTER = 'Aether'
BATCHSIZE = 1000

con = sl.connect('market.db')

def constructHTTP(ids: list[int], context: str, start: int, listings = 0, hq = "") -> str:
    url = 'https://universalis.app/api/' + context + '/' + ','.join(str(i) for i in ids[start:start + BATCHSIZE]) + '?listings=' + str(listings)
    if(hq):
        url += '&hq=' + hq
    return url

def getWorldPrices(ids: list[int]):
    index = 0
    while(index < len(ids)):
        url = constructHTTP(ids, WORLD, index)
        response = requests.get(url)
        
        if(response.ok):
            itemList = response.json()['items']
            for item in itemList:
                with con:
                    con.execute("""
                        INSERT INTO Prices (id, high_quality, world_price, trade_velocity) 
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT(id_quality) 
                        DO UPDATE SET 
                            world_price=excluded.world_price, 
                            trade_velocity=excluded.world_price;
                    """, (item['itemID'], 0, item['minPriceNQ'], item['nqSaleVelocity']))
                    con.execute("""
                        INSERT INTO Prices (id, high_quality, world_price, trade_velocity) 
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT(id_quality) 
                        DO UPDATE SET 
                            world_price=excluded.world_price, 
                            trade_velocity=excluded.world_price;
                    """, (item['itemID'], 1, item['minPriceHQ'], item['hqSaleVelocity']))
        index += BATCHSIZE

def getDCPrices(ids: list[int]):
    index = 0
    while(index < len(ids)):
        url = constructHTTP(ids, DATACENTER, index, listings=1, hq='false')
        response = requests.get(url)
        
        if(response.ok):
            itemList = response.json()['items']
            for item in itemList:
                if(item['listings']):
                    with con:
                        con.execute("""
                            UPSERT INTO Prices (id, high_quality, dc_price, cheapest_world) 
                            VALUES (?, ?, ?, ?)
                            ON CONFLICT(id_quality) 
                            DO UPDATE SET 
                                dc_price=excluded.dc_price, 
                                cheapest_world=excluded.cheapest_world;
                        """, (item['itemID'], 0, item['listings'][0]['pricePerUnit'], item['listings'][0]['worldName']))

        url = constructHTTP(ids, DATACENTER, index, listings=1, hq='true')
        response = requests.get(url)
        
        if(response.ok):
            itemList = response.json()['items']
            for item in itemList:
                if(item['listings']):
                    with con:
                        con.execute("""
                            UPSERT INTO Prices (id, high_quality, dc_price, cheapest_world) 
                            VALUES (?, ?, ?, ?)
                            ON CONFLICT(id_quality) 
                            DO UPDATE SET 
                                dc_price=excluded.dc_price, 
                                cheapest_world=excluded.cheapest_world;
                        """, (item['itemID'], 1, item['listings'][0]['pricePerUnit'], item['listings'][0]['worldName']))


        index += BATCHSIZE

def itemIdToName(id: int) -> str:
    response = requests.get('https://xivapi.com/item/' + str(id) + '?columns=Name')
    if(response.ok):    
        return response.json()['Name']
    return 'XIVAPI Name Lookup Failed'


def updatePrices():
    ids = requests.get('https://universalis.app/api/marketable').json()
    worldPrices = getWorldPrices(ids)
    print('World Prices Updated')
    dcPrices = getDCPrices(ids)
    print('DC Prices Updated')
    
    
updatePrices()


# counterNQ = Counter()
# counterHQ = Counter()
# for itemID in dcPrices: 
#     counterNQ[itemID] = worldPrices[itemID][0] - dcPrices[itemID][0]
#     counterHQ[itemID] = worldPrices[itemID][1] - dcPrices[itemID][1]

# print('NQ:')
# for k, v in counterNQ.most_common(10):
#     print(itemIdToName(k) + ' - profit: ' + str(v))

    
# print('\n\nHQ:')
# for k, v in counterHQ.most_common(10):
#     print(itemIdToName(k) + ' - profit: ' + str(v))