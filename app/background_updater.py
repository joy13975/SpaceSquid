import sys
import requests
from datetime import datetime
from random import random
from time import sleep

from util import *

jobs_file = 'bg_jobs.json'


def update_coin_prices(coin_prices_csv, *coins):
    coins_str = ','.join(coins)
    r = requests.get(url=f'https://api.coingecko.com/api/v3/simple/price?ids={coins_str}&vs_currencies=usd')
    assert r.status_code == 200
    df = pd.DataFrame([(k, v['usd']) for k, v in r.json().items()], columns=['coin', 'usd'])
    df['LastUpdate'] = datetime.now().isoformat()
    df.to_csv(coin_prices_csv, index=False)


def update_nft_rewards(nft_rewards_csv):
    nftlookup_io = initialize_nftlookup_io()
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

    while True:
        r = requests.post(url='https://nftlookup.io/nftlookup/GeneralComponents/DatabaseFunctions.cfc',
                          headers=h, data=data)
        if r.status_code == 200:
            break
        print(f'Failed to fetch rewards: {r.reason}, waiting...')
        sleep(60)
    r_data = r.json()
    df = pd.DataFrame(r_data['TableData'], columns=[v['title'] for v in r_data['TableColumns']])\
        .rename(columns={
            'Item Name': 'name',
            'Rarity (Rewards)': 'reward'
        })
    df['LastUpdate'] = datetime.now().isoformat()
    df.to_csv(nft_rewards_csv, index=False)


def update_nft_prices(nft_prices_csv, nft_rewards_csv, coin_prices_csv, force_update_token_ids):
    token_ids = []
    if not force_update_token_ids and os.path.isfile(nft_prices_csv):
        token_ids = pd.read_csv(nft_prices_csv).token_id.values
    # need reward names to filter opensea items
    rewards = load_file(nft_rewards_csv, load_func=pd.read_csv, patient=True)
    reward_item_names = lowertrim(rewards.name).values
    assets = fetch_opensea_assets(reward_item_names, token_ids=token_ids)
    coin_prices = load_file(coin_prices_csv, load_func=pd.read_csv, patient=True)
    prices = get_nft_prices(assets, coin_prices)
    prices['LastUpdate'] = datetime.now().isoformat()
    prices.to_csv(nft_prices_csv, index=False)


if __name__ == '__main__':
    assert len(sys.argv) > 1, 'Provide function name'
    func_name = sys.argv[1]
    sleep(random() * 2)
    jobs = read_json(jobs_file)
    if func_name in jobs:
        print(f'Skipping {func_name} because it is ongoing')
        exit(0)
    jobs.append(func_name)
    jobs = sorted(set(jobs))
    write_json(jobs_file, jobs)
    print(f'Starting function {func_name}')
    locals()[func_name](*sys.argv[2:])
    jobs.remove(func_name)
    write_json(jobs_file, jobs)
