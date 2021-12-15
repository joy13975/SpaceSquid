import requests
from urllib.parse import urlencode
from multiprocessing.pool import ThreadPool

import numpy as np
import pandas as pd

from time import sleep

nan = float('nan')
def get_order_eth_price(order):
    return float(order['current_price']) * \
        float(order['payment_token_contract']['eth_price']) \
            / (10 ** order['payment_token_contract']['decimals']) \
            / int(order['quantity'])

def get_order_usd_price(order):
    return float(order['current_price']) * \
        float(order['payment_token_contract']['usd_price']) \
            / (10 ** order['payment_token_contract']['decimals']) \
            / int(order['quantity'])

def fetch_prices(assets):
    data = []
    for a in assets:
        sell_orders = a['sell_orders']
        cheapest_so_idx = np.argmin([get_order_usd_price(so) for so in sell_orders])
        cheapest_so = sell_orders[cheapest_so_idx]
        cheapest_price_eth = get_order_eth_price(cheapest_so)
        cheapest_price_usd = get_order_usd_price(cheapest_so)
        cheapest_so_quantity = int(cheapest_so['quantity'])
        data.append([
            a['name'],
            a['permalink'],
            cheapest_price_eth,
            cheapest_price_usd,
            cheapest_so_quantity,
        ])
    return pd.DataFrame(data, columns=['Name', 'Link', 'ETH', 'USD', 'Qty'])

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
            r = requests.get(url=f'https://api.opensea.io/api/v1/assets?{params}')
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
