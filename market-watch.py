import requests
import asyncio
from collections import Counter
import sqlite3 as sl

#TODO: 99x quanitity,
WORLD = 'Cactuar'
DATACENTER = 'Aether'
BATCHSIZE = 100

con = sl.connect('market.db')

def constructHTTP(ids: list[int], context: str, start: int, listings = 0, hq = "") -> str:
    url = 'https://universalis.app/api/' + context + '/' + ','.join(str(i) for i in ids[start:start + BATCHSIZE]) + '?listings=' + str(listings)
    if(hq):
        url += '&hq=' + hq
    return url

def getWorldPrices(ids: list[int]):
    index = 0
    while(index < len(ids)):
        print("\rQuerying " + str(index) + " - " + str(index + BATCHSIZE) + "...")
        url = constructHTTP(ids, WORLD, index)
        response = requests.get(url)
        
        if(response.ok):
            itemList = response.json()['items']
            for item in itemList:
                with con:
                    con.execute("""
                        INSERT INTO Prices (id, high_quality, world_price, trade_velocity) 
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT(id, high_quality) 
                        DO UPDATE SET 
                            world_price=excluded.world_price, 
                            trade_velocity=excluded.trade_velocity;
                    """, (item['itemID'], 0, item['minPriceNQ'], item['nqSaleVelocity']))
                    con.execute("""
                        INSERT INTO Prices (id, high_quality, world_price, trade_velocity) 
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT(id, high_quality) 
                        DO UPDATE SET 
                            world_price=excluded.world_price, 
                            trade_velocity=excluded.trade_velocity;
                    """, (item['itemID'], 1, item['minPriceHQ'], item['hqSaleVelocity']))
        index += BATCHSIZE

def getDCPrices(ids: list[int]):
    index = 0
    while(index < len(ids)):
        print("\rQuerying " + str(index) + " - " + str(index + BATCHSIZE) + "...")

        url = constructHTTP(ids, DATACENTER, index, listings=1, hq='false')
        response = requests.get(url)
        
        if(response.ok):
            itemList = response.json()['items']
            for item in itemList:
                if(item['listings']):
                    with con:
                        con.execute("""
                            INSERT INTO Prices (id, high_quality, dc_price, cheapest_world) 
                            VALUES (?, ?, ?, ?)
                            ON CONFLICT(id, high_quality) 
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
                            INSERT INTO Prices (id, high_quality, dc_price, cheapest_world) 
                            VALUES (?, ?, ?, ?)
                            ON CONFLICT(id, high_quality) 
                            DO UPDATE SET 
                                dc_price=excluded.dc_price, 
                                cheapest_world=excluded.cheapest_world;
                        """, (item['itemID'], 1, item['listings'][0]['pricePerUnit'], item['listings'][0]['worldName']))


        index += BATCHSIZE


def createView():
    with con:
        con.execute("DROP VIEW IF EXISTS PriceDiff;")
        con.execute("""
            CREATE VIEW PriceDiff AS
            SELECT id, name, high_quality, world_price, dc_price, cheapest_world, (world_price-dc_price) as profit, 1.0*(world_price-dc_price)/(1.0*dc_price) as percent, trade_velocity
            FROM Prices
            WHERE trade_velocity >= 1;
        """)

def itemIdToName(id: int) -> str:
    cursor = con.execute("SELECT name FROM Prices WHERE id = ?", (id,))
    name = cursor.fetchone()[0]
    if (name):
        return name
    response = requests.get('https://xivapi.com/item/' + str(id) + '?columns=Name')
    if(response.ok):    
        name = response.json()['Name']
        with con:
            con.execute("""
                INSERT INTO Prices (id, high_quality, name) 
                    VALUES (?, ?, ?)
                    ON CONFLICT(id, high_quality) 
                    DO UPDATE SET 
                        name=excluded.name;
            """, (id, 0, name))
            con.execute("""
                INSERT INTO Prices (id, high_quality, name) 
                    VALUES (?, ?, ?)
                    ON CONFLICT(id, high_quality) 
                    DO UPDATE SET 
                        name=excluded.name;
            """, (id, 1, name))
        return name
    return 'XIVAPI Name Lookup Failed'


def updatePrices():
    ids = requests.get('https://universalis.app/api/marketable').json()
    print(str(len(ids)) + ' items to be queried\n')
    worldPrices = getWorldPrices(ids)
    print('World Prices Updated\n')
    dcPrices = getDCPrices(ids)
    print('DC Prices Updated\n')
    createView()
    
def getTopFlips(n: int):
    with con:
        cursor = con.execute("SELECT * FROM PriceDiff ORDER BY profit DESC")
        items = cursor.fetchall()
        for item in items:
            print(itemIdToName(item[0]))