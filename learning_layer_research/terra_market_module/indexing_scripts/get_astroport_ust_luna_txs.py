import requests
import json
import base64
import asyncio
import aiohttp
import numpy as np
import pandas as pd

try:
    swap_txs = pd.read_csv("../astroport_txs_data/astroport_swap_txs12.csv")
except:
    swap_txs = pd.DataFrame([], columns = ['TxHash','BlockHeight','UserAddress','offer_asset','offer_amount','ask_asset','return_amount','tax_amount','spread_amount','commission_amount','maker_fee_amount','belief_price' ])

astroport_lunc_ustc_pool = "terra1m6ywlgn6wrjuagcmmezzz2a029gtldhey5k552"


async def get_luna_ust_txs( debug=True):

    offset = 259695200  
    session = aiohttp.ClientSession()

    while(1):
        tasks = []

        for i in range(0, 250):
            task = asyncio.ensure_future(fetch_data(session, offset))
            tasks.append(task)
            offset = offset - 100

        responses = await asyncio.gather(*tasks, return_exceptions=True)
        exceptions = [res for res in responses if isinstance(res, Exception)]
        successful_results = [ res for res in responses if not isinstance(res, Exception)]

        print(f'fetch_data -::- Finished successfully: {successful_results}')
        print(f'fetch_data -::- Threw exceptions: {exceptions}')

        swap_txs.to_csv(
            f'../astroport_txs_data/astroport_swap_txs12.csv', index=False)


async def fetch_data(session, offset):
    try:
        URL = "https://fcd.terra.dev/v1/txs?offset=" + str(offset) + "&account=" + astroport_lunc_ustc_pool + "&limit=100"
        # response = requests.get(url = URL).json()
        # print(URL)
        print(URL)

        while(1):
            async with session.get(URL) as response_:
                try:
                    response = await response_.json(content_type=None)
                    break
                except Exception as e:
                    print(e)
                    await asyncio.sleep(4)
                    continue

        for tx in response['txs']:
            try:
                txhash = tx['txhash']
                block_height = tx['height']
                # print(txhash)

                offer_amount = 0
                return_amount = 0
                tax_amount = 0
                spread_amount = 0
                commission_amount = 0
                maker_fee_amount = 0
                belief_price = 0


                # IF TX FAILED // EXCEEDS BLOCK HEIGHT
                if not tx.get('logs'):
                    continue

                # PROCESS MSGs
                msg_number = 0
                for msg in tx['tx']['value']['msg']:
                    if msg['type'] == "wasm/MsgExecuteContract":
                        sender =  msg['value']['sender']
                        coins = msg['value']['coins']
                        contract = msg['value']['contract']

                        if type(msg['value']['execute_msg']).__name__ == "dict" :
                            decodedMsg =   json.loads ( json.dumps(msg['value']['execute_msg']) )
                        else:
                            decodedMsg = json.loads( base64.b64decode(msg['value']['execute_msg']).decode("utf-8") )
                        
                        # SWAP TX
                        if decodedMsg.get('swap')!= None and  contract == astroport_lunc_ustc_pool:
                            # Belief price
                            if decodedMsg['swap'].get('belief_price'):
                                belief_price = decodedMsg['swap']['belief_price']

                            events = tx['logs'][msg_number]['events']
                            for event in events:
                                if event['type'] == "wasm":
                                    for attribute in event['attributes']:
                                        if attribute['key'] == 'offer_asset':
                                            offer_asset = attribute['value']
                                        if attribute['key'] == 'ask_asset':
                                            ask_asset = attribute['value']
                                        if attribute['key'] == 'offer_amount':
                                            offer_amount = attribute['value']
                                        if attribute['key'] == 'return_amount':
                                            return_amount = attribute['value']
                                        if attribute['key'] == 'tax_amount':
                                            tax_amount = attribute['value']
                                        if attribute['key'] == 'spread_amount':
                                            spread_amount = attribute['value']
                                        if attribute['key'] == 'commission_amount':
                                            commission_amount = attribute['value']
                                        if attribute['key'] == 'maker_fee_amount':
                                            maker_fee_amount = attribute['value']

                            swap_txs.loc[len(swap_txs.index)] = [txhash,block_height,sender,offer_asset,offer_amount, ask_asset , return_amount , tax_amount, spread_amount , commission_amount , maker_fee_amount  ,belief_price ]

                    msg_number = msg_number + 1

            except Exception as e:
                print(e)

        print(f"offset = {offset} block_height = {block_height}")

    except Exception as e:
        print(e)



asyncio.run(get_luna_ust_txs())

# get_luna_ust_txs()