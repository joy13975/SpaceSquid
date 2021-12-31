import requests
from urllib.parse import urlencode
import json
import numpy as np
import pandas as pd
from multiprocessing.pool import ThreadPool
from http.cookies import SimpleCookie
import re

from time import sleep

with open('config.json', 'r') as f:
    config = json.loads(f.read())

nan = float('nan')

opensea_commission = 0.025
opensea_townstar_commission = 0.025

def lowertrim(s):
    if isinstance(s, pd.Series):
        return s.str.lower().str.strip()
    else:
        return s.lower().strip()

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

nftlookup_io = initialize_nftlookup_io()

def fetch_rewards():
    h = {
        'authority': 'nftlookup.io',
        'pragma': 'no-cache',
        'cache-control': 'no-cache',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'x-requested-with': 'XMLHttpRequest',
        'origin': 'https://nftlookup.io',
        'referer': 'https://nftlookup.io/nftlookup/NFT_index.cfm',
        'cookie': f'CFID={nftlookup_io["CFID"]}; CFTOKEN={nftlookup_io["CFTOKEN"]}',
    }
    data = ('method=getTempTableInfo&curJson=%7B%22BasedOn%22%3A%22Items%22%2C%22DashBoardItemName%22%3A%22NFT+Rewards+for+Townstar+(Vox+%26+Townstar+NFT)%22%2C%22DashBoardItemDescription%22%3A%22This+dashboard+will+give+you+all+NFT\'s+that+are+used+in+Townstar%2C+ordered+by+their+rarity.+It+includes+as+well+the+current+price+on+the+market%22%2C%22DashboardID%22%3A%22-1%22%2C%22IsItemActivated%22%3A%221%22%2C%22Collections%22%3A%5B%22collectvox%22%2C%22town-star%22%5D%2C%22CollectionItems%22%3A%5B%5D%2C%22SelectFields%22%3A%5B%22itemname%22%2C%22colname%22%2C%22itemrarityscore%22%2C%22reppricelowETH%22%5D%2C%22Statements%22%3A%5B%5D%2C%22Footer%22%3A%5B%5D%2C%22FinalStatements%22%3A%5B%22itemname%22%2C%22colname%22%2C%22itemrarityscore%22%2C%22reppricelowETH%22%5D%2C%22OrderBy%22%3A%5B%22itemrarityscoreDesc%22%5D%2C%22LimitQuery%22%3A%5B%7B%22AndOr%22%3A%22And%22%2C%22FieldName%22%3A%22itemrarityscore%22%2C%22Type%22%3A%22Number%22%2C%22Operator%22%3A%22%3E%22%2C%22Value%22%3A%220%22%7D%5D%2C%22isGraph%22%3A%220%22%2C%22GraphType%22%3A%22%22%2C%22GraphTitle%22%3A%22%22%2C%22GraphLabel%22%3A%22%22%2C%22GraphValues%22%3A%22%22%2C%22isShowDetailsButton%22%3A%221%22%2C%22Alert%22%3A%5B%5D%7D&'
        f'curToken={nftlookup_io["curToken"]}')
    r = requests.post(url='https://nftlookup.io/nftlookup/GeneralComponents/DatabaseFunctions.cfc',
        headers=h, data=data)
    r_data = r.json()
    rewards_df = pd.DataFrame(r_data['TableData'], columns=[v['title'] for v in r_data['TableColumns']])\
        .rename(columns={
            'Item Name': 'name',
            'Rarity (Rewards)': 'reward'
        })
    return rewards_df

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
            fee *= fetch_coin_prices('ethereum')['ethereum']
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
            fee *= fetch_coin_prices('ethereum')['ethereum']
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
            os_last_sale_usd = parse_last_sale_price(last_sale, symbol='usd')
            # last_sale_qty = float(last_sale['quantity'])
            gs_link, gs_usd, gs_qty = fetch_gala_store_price(cheapest_so, name=a['name'])
        else:
            print(f'Warning: {a["name"]} has no sell orders')
        return [
            a['name'],
            a['permalink'],
            os_price_eth,
            os_price_usd,
            max(os_qty, 0),
            100 * (os_price_usd - os_last_sale_usd) / os_last_sale_usd,
            gs_link,
            gs_usd,
            max(gs_qty, 0),
            os_price_usd * (1 - opensea_commission - opensea_townstar_commission) -
                (gs_usd + gs_fee + gs_mint_fee)
        ]
    with ThreadPool(n_workers) as pool:
        data = [i for i in pool.map(thread_work, assets) if i is not None]
    return pd.DataFrame(data, columns=[
        'Name', 'OS Link',
        'OS ETH', 'OS USD', 'OS Qty', 'OS Change',
        'GS Link', 'GS USD', 'GS Qty', 'Arb'])


def fetch_coin_prices(*coins):
    coins_str = ','.join(coins)
    r = requests.get(url=f'https://api.coingecko.com/api/v3/simple/price?ids={coins_str}&vs_currencies=usd')
    assert r.status_code == 200
    return {k: v['usd'] for k, v in r.json().items()}


def fetch_items(
        update_status_callback=None,
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
        if callable(update_status_callback):
            update_status_callback(f'Fetching offset={offset}')
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
                if callable(attribute_filter) and not attribute_filter(a):
                    continue
                if has_desired_trait(a['traits']):
                    yield a
            sleep(0.1)
        if len(token_ids) > 0 or len(assets) == 0:
            # token_ids loop would have gone through all pages by itself
            # because token_id_blocksize <= limit
            break
        offset += limit
