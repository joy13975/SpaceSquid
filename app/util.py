import requests
from urllib.parse import urlencode
import json
import numpy as np
import pandas as pd
from multiprocessing.pool import ThreadPool

from time import sleep

with open('config.json', 'r') as f:
    config = json.loads(f.read())

nan = float('nan')

opensea_commission = 0.025
opensea_townstar_commission = 0.025

def fetch_gala_store_price(sell_order, symbol_preference=['TOWN', 'GALA', 'ETH', 'BAT']):
    h = {
        "content-type": "application/json",
        "pragma": "no-cache",
    }
    q = '''
        query gameItemProducts($gameItemProductsInput: GameItemProductsInput) {
            gameItemProducts(gameItemProductsInput: $gameItemProductsInput) {
                baseId
                name
                description
                game
                qtyLeft
                purchasingDisabled
                expiresAt
                prices {
                price
                basePrice
                usdPriceInCents
                usdBasePriceInCents
                symbol
                }
            }
        }
    '''
    gala_item_id = sell_order['calldata'][166:][:36]
    d = json.dumps(
        {
            "operationName": "gameItemProducts",
            "variables": {
                "gameItemProductsInput": {
                    "baseId": f"0x{gala_item_id}"
                }
            },
            "query": q
        }
    )
    r = requests.post(url='https://walletsrv.gala.games/graphql', headers=h, data=d)
    usd_price = nan
    qty = nan
    effective_symbol = None
    if r.status_code == 200:
        try:
            data = r.json()['data']['gameItemProducts'][0]
            symbols = []
            qty = float(data['qtyLeft'])
            for price_data in data['prices']:
                sym = price_data['symbol']
                symbols.append(sym)
                for pref_sym in symbol_preference:
                    if sym.lower() == pref_sym.lower():
                        usd_price = float(price_data['usdPriceInCents'])/100
                        effective_symbol = pref_sym
                        break
            if usd_price is nan:
                print(f'No {symbol_preference} price available in Gala Store for {gala_item_id}: only {symbols}')
        except IndexError:
            print(f'No {symbol_preference} price available in Gala Store for {gala_item_id}')
    else:
        print(f'Failed to fetch Gala Store price for {gala_item_id}: {r.reason}')
    gs_link = f'https://app.gala.games/games/buy-item/0x{gala_item_id}/?currency={effective_symbol}'
    return gs_link, usd_price, qty


def query_gala(data):
    h = {
        'pragma': 'no-cache',
        'content-type': 'application/json',
        'cookie': f'blankUser={config["gala_store_blank_user"]}'
    }
    return requests.post(url='https://walletsrv.gala.games/gateway', headers=h, data=json.dumps(data))


def fetch_gala_store_txn_fee(in_usd=True, symbol='TOWN'):
    q = '''
        query transactionFeeEstimate($symbol: String!, $transactionHash: String) {
            transactionFeeEstimate(symbol: $symbol, transactionHash: $transactionHash) {
                gasUnitsEstimate
                gasPriceEstimate {
                    high
                    suggested
                    low
                }
            }
        }
    '''
    d = {
        'operationName': 'transactionFeeEstimate',
        'variables': {'symbol': symbol},
        'query': q
    }
    r = query_gala(d)
    fee = nan
    if r.status_code == 200:
        data = r.json()['data']['transactionFeeEstimate']
        fee = float(data['gasUnitsEstimate']) * float(data['gasPriceEstimate']['low']) / 1e18
        if in_usd:
            fee *= get_coin_price('ethereum')['ethereum']
    else:
        print('Failed to get Gala Store transaction fee')
    return fee


def fetch_gala_mint_fee(in_usd=True):
    q = '''
        query getTokenClaimFees($networks: [ClaimNetwork!]) {
        tokenClaimFees(networks: $networks) {
            network
            currency
            expires
            contractTypes {
            contractType
            nonFungible {
                minBatchFee
                perTokenFee
                maxBatchSize
            }
            }
        }
        }
    '''
    d = {
        "operationName": "getTokenClaimFees",
        "variables": {},
        "query": q
    }
    r = query_gala(d)
    if r.status_code == 200:
        data = next(i for i in r.json()['data']['tokenClaimFees'][0]['contractTypes']
                    if i['contractType'] == 'erc1155')['nonFungible']
        send_amount = float(data['minBatchFee']) - float(data['perTokenFee'])
        txn_fee = fetch_gala_store_txn_fee(in_usd=False, symbol='ITEM')
        fee = send_amount + txn_fee
        if in_usd:
            fee *= get_coin_price('ethereum')['ethereum']
    else:
        print('Failed to get Gala Store miting fee')
    return fee


def parse_sell_order_price(order, symbol='usd'):
    return float(order['current_price']) * \
        float(order['payment_token_contract'][f'{symbol}_price']) \
        / (10 ** order['payment_token_contract']['decimals']) \
        / int(order['quantity'])



def parse_last_sale_price(sale, symbol='usd'):
    return float(sale['total_price']) * \
        float(sale['payment_token'][f'{symbol}_price']) \
        / (10 ** sale['payment_token']['decimals']) \
        / int(sale['quantity'])



def parse_prices(assets, n_workers=16):
    gs_fee = fetch_gala_store_txn_fee()
    gs_mint_fee = fetch_gala_mint_fee()

    def thread_work(a):
        sell_orders = a['sell_orders']
        if not sell_orders:
            print(a['name'] + ' has no sell orders')
            return None
        cheapest_so_idx = np.argmin([parse_sell_order_price(so, symbol='usd') for so in sell_orders])
        cheapest_so = sell_orders[cheapest_so_idx]
        cheapest_price_eth = parse_sell_order_price(cheapest_so, symbol='eth')
        cheapest_price_usd = parse_sell_order_price(cheapest_so, symbol='usd')
        cheapest_so_qty = int(cheapest_so['quantity'])
        last_sale = a['last_sale']
        # last_sale_eth = parse_last_sale_price(last_sale, symbol='eth')
        last_sale_usd = parse_last_sale_price(last_sale, symbol='usd')
        # last_sale_qty = float(last_sale['quantity'])
        print(f'Fetching price for {a["name"]} ({a["permalink"]})')
        gs_link, gs_usd, gs_qty = fetch_gala_store_price(cheapest_so)
        return [
            a['name'],
            a['permalink'],
            cheapest_price_eth,
            cheapest_price_usd,
            max(cheapest_so_qty, 0),
            100 * (cheapest_price_usd - last_sale_usd) / last_sale_usd,
            gs_link,
            gs_usd,
            max(gs_qty, 0),
            max(0, cheapest_price_usd * (1 - opensea_commission - opensea_townstar_commission) -
                (gs_usd + gs_fee + gs_mint_fee)) if gs_qty > 0 else nan
        ]
    with ThreadPool(n_workers) as pool:
        data = [i for i in pool.map(thread_work, assets) if i is not None]
    return pd.DataFrame(data, columns=[
        'Name', 'OS Link',
        'OS ETH', 'OS USD', 'OS Qty', 'OS Change',
        'GS Link', 'GS USD', 'GS Qty', 'Arb'])


def get_coin_price(*coins):
    coins_str = ','.join(coins)
    r = requests.get(url=f'https://api.coingecko.com/api/v3/simple/price?ids={coins_str}&vs_currencies=usd')
    assert r.status_code == 200
    return {k: v['usd'] for k, v in r.json().items()}


def fetch_rewards(url=r'https://docs.google.com/spreadsheets/d/1z_18FrAbA9gMbyGn91j_Uf31lQW0ksUUHpPuGrXNHQY/export?format=csv&id=1z_18FrAbA9gMbyGn91j_Uf31lQW0ksUUHpPuGrXNHQY&gid=0'):
    return pd.read_csv(url)


def fetch_items(
        st_empty=None,
        collection='town-star',
        contract_address='0xc36cf0cfcb5d905b8b513860db0cfe63f6cf9f5c',
        attribute_filter=None,
        trait_filter=lambda df: df.game == 'Town Star',
        token_ids=[]):
    offset = 0
    limit = 50  # Max for opensea API
    token_id_blocksize = 30
    assert token_id_blocksize <= limit  # Because if not, paging loop will be needed

    def has_desired_trait(traits):
        if trait_filter is None:
            return True
        df = pd.DataFrame(traits)
        try:
            df = df.set_index('trait_type')[['value']].T
            return trait_filter(df.iloc[0])
        except (KeyError, AttributeError):
            return False
    # Paging loop
    while True:
        base_params = urlencode(dict(
            collection=collection,
            asset_contract_address=contract_address,
            order_direction='desc',
            limit=limit,
            offset=offset
        ))
        if st_empty:
            st_empty.write(f'Fetching offset={offset}')
        ti_param_list = ['']
        if len(token_ids) > 0:
            ti_param_list = [
                '&'.join(f'token_ids={ti}' for ti in token_ids[ti_block:ti_block+token_id_blocksize])
                for ti_block in range(0, len(token_ids), token_id_blocksize)
            ]
        # token_ids loop
        for ti_param in ti_param_list:
            params = base_params
            if ti_param:
                params += '&' + ti_param
            status_code = None
            while status_code != 200:
                r = requests.get(url=f'https://api.opensea.io/api/v1/assets?{params}')
                status_code = r.status_code
                if 'Gateway Time-out' in r.reason:
                    print(f'Gateway Time-out (params={params}); sleeping 60s...')
                    sleep(60)

            assert r.status_code == 200, r.reason
            assets = r.json()['assets']
            if len(assets) == 0:
                break
            for a in assets:
                if attribute_filter is not None and not attribute_filter(a):
                    continue
                if has_desired_trait(a['traits']):
                    yield a
            sleep(0.1)
        if len(token_ids) > 0 or len(assets) == 0:
            # token_ids loop would have gone through all pages by itself
            # because token_id_blocksize <= limit
            break
        offset += limit
