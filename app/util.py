import requests
import os
import json
import numpy as np
import pandas as pd
from multiprocessing.pool import ThreadPool
from http.cookies import SimpleCookie
import re
from urllib.parse import urlencode
from datetime import datetime, timedelta

from time import sleep

with open('config.json', 'r') as f:
    config = json.loads(f.read())

nan = float('nan')

opensea_commission = 0.025
opensea_townstar_commission = 0.025

def has_expired(file, threshold_sec, load_func=pd.read_csv):
    df = load_file(file, load_func=load_func)
    if df is None:
        # No data = much fetch = expired
        return True
    last_update = datetime.fromisoformat(df.iloc[0].LastUpdate)
    return datetime.now() > (last_update + timedelta(seconds=threshold_sec))


def load_file(file, load_func, patient=False, wait_sec=1):
    while patient and not os.path.isfile(file):
        sleep(wait_sec)
    return load_func(file) if os.path.isfile(file) else None


def read_json(file):
    with open(file, 'r') as f:
        return json.load(f)


def write_json(file, data):
    with open(file, 'w') as f:
        json.dump(data, f)


def lowertrim(s):
    if isinstance(s, pd.Series):
        return s.str.lower().str.strip()
    else:
        return s.lower().strip()


def fetch_opensea_assets(
        reward_item_names,
        token_ids=[],
        collection='town-star',
        contract_address='0xc36cf0cfcb5d905b8b513860db0cfe63f6cf9f5c'):
    offset = 0
    limit = 50  # Max for opensea API
    token_id_blocksize = 30
    assert token_id_blocksize <= limit  # Because if not, paging loop will be needed
    wanted_assets = []
    # Paging loop
    while True:
        base_params = urlencode(dict(
            collection=collection,
            asset_contract_address=contract_address,
            order_direction='desc',
            limit=limit,
            offset=offset
        ))
        print(f'Fetching offset={offset}')
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
                if not ('name' in a and
                        isinstance(a['name'], str) and
                        any(lowertrim(a['name']) in n for n in reward_item_names)):
                    continue
                df = pd.DataFrame(a['traits'])
                try:
                    df = df.set_index('trait_type')[['value']].T
                    if df.iloc[0].game == 'Town Star':
                        wanted_assets.append(a)
                except (KeyError, AttributeError):
                    continue
            sleep(0.1)
        if len(token_ids) > 0 or len(assets) == 0:
            # token_ids loop would have gone through all pages by itself
            # because token_id_blocksize <= limit
            break
        offset += limit
    return wanted_assets


def get_nft_prices(assets, coin_prices):
    gs_fee = fetch_gala_store_txn_fee(coin_prices)
    gs_mint_fee = fetch_gala_mint_fee(coin_prices)

    def thread_work(a):
        sell_orders = a['sell_orders']
        os_price_eth = nan
        os_price_usd = nan
        os_qty = nan
        os_last_sale_usd = nan
        gs_link = 'N/A'
        gs_usd = nan
        gs_qty = nan
        if sell_orders:
            cheapest_so_idx = np.argmin([parse_sell_order_price(so, symbol='usd') for so in sell_orders])
            cheapest_so = sell_orders[cheapest_so_idx]
            os_price_eth = parse_sell_order_price(cheapest_so, symbol='eth')
            os_price_usd = parse_sell_order_price(cheapest_so, symbol='usd')
            os_qty = int(cheapest_so['quantity'])
            last_sale = a['last_sale']
            # last_sale_eth = parse_last_sale_price(last_sale, symbol='eth')
            os_last_sale_usd = parse_last_sale_price(last_sale, symbol='usd') if last_sale else nan
            # last_sale_qty = float(last_sale['quantity'])
            gs_link, gs_usd, gs_qty = fetch_gala_store_price(cheapest_so, name=a['name'])
        else:
            print(f'Warning: {a["name"]} has no sell orders')
        return [
            a['token_id'],
            a['name'],
            a['permalink'],
            os_price_eth,
            os_price_usd,
            os_last_sale_usd,
            max(os_qty, 0),
            100 * (os_price_usd - os_last_sale_usd) / os_last_sale_usd,
            gs_link,
            gs_usd,
            max(gs_qty, 0),
            min(os_last_sale_usd, os_price_usd) * (1 - opensea_commission - opensea_townstar_commission) -
            (gs_usd + gs_fee + gs_mint_fee)
        ]
    with ThreadPool(16) as pool:
        data = [i for i in pool.map(thread_work, assets) if i is not None]

    return pd.DataFrame(data, columns=[
        'token_id',
        'Name', 'OS Link',
        'OS ETH', 'OS USD', 'OS LastSale USD', 'OS Qty', 'OS Change',
        'GS Link', 'GS USD', 'GS Qty', 'Arb'])


def initialize_nftlookup_io():
    r = requests.get('https://nftlookup.io/nftlookup/NFT_index.cfm')
    # Get CFID and CFTOKEN
    cookiestr = re.sub(
        'HttpOnly(, )?', '',
        r.headers['Set-Cookie'])
    cookie = SimpleCookie()
    cookie.load(cookiestr)
    # Get curToken
    html = r.content.decode()
    cur_token = re.search('curToken: \'(.+)\'', html).group(1)
    return dict(curToken=cur_token, **{
        k: v.value for k, v in cookie.items()
    })


def fetch_gala_store_price(sell_order, name, symbol_preference=['TOWN', 'GALA', 'ETH', 'BAT']):
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
                print(f'No {symbol_preference} price available in Gala Store for {name}: only {symbols}')
        except IndexError:
            print(f'No price available in Gala Store for {name}')
    else:
        print(f'Failed to fetch Gala Store price for {name}: {r.reason}')
    gs_link = f'https://app.gala.games/games/buy-item/0x{gala_item_id}/?currency={effective_symbol}'
    return gs_link, usd_price, qty


def query_gala(data):
    h = {
        'pragma': 'no-cache',
        'content-type': 'application/json',
        'cookie': f'blankUser={config["gala_store_blank_user"]}'
    }
    return requests.post(url='https://walletsrv.gala.games/gateway', headers=h, data=json.dumps(data))


def fetch_gala_store_txn_fee(coin_prices, in_usd=True, symbol='TOWN'):
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
            fee *= coin_prices[coin_prices.coin == 'ethereum'].iloc[0].usd
    else:
        print('Failed to get Gala Store transaction fee')
    return fee


def fetch_gala_mint_fee(coin_prices, in_usd=True):
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
        txn_fee = fetch_gala_store_txn_fee(coin_prices, in_usd=False, symbol='ITEM')
        fee = send_amount + txn_fee
        if in_usd:
            fee *= coin_prices[coin_prices.coin == 'ethereum'].iloc[0].usd
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
