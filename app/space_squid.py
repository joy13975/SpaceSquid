import os
from datetime import datetime, timedelta
import base64
import subprocess

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from util import *
from background_state import BackgroundStateDB

st.set_page_config(page_title='SpaceSquid', layout='wide')

page_refresh_interval = 30  # seconds
refresh_count = st_autorefresh(interval=page_refresh_interval * 1000)
page_last_refresh_time = datetime.now()

base_data_dir = 'data/town-star'
nft_prices_csv = os.path.join(base_data_dir, 'nft_prices.csv')
nft_rewards_csv = os.path.join(base_data_dir, 'nft_rewards.csv')
coin_prices_csv = os.path.join(base_data_dir, 'coin_prices.csv')

st.markdown(
    f'''
        <style>
            section[data-testid="stSidebar"] {{
                width: 40%;
            }}
            section[data-testid="stSidebar"] > div {{
                width: 100%;
            }}
            table {{
                width: 100%;
            }}
            button[kind="primary"] {{
                width: 100%;
            }}
        </style>
    ''',
    unsafe_allow_html=True
)

with st.sidebar:
    status_text = st.empty()
    def update_status(s):
        status_text.write('## Status: ' + s)

def load_data(file, bg_args=[], load_func=pd.read_csv, force_update=False, patient=False):
    if force_update or not os.path.isfile(file):
        subprocess.Popen(['python', 'app/background_updater.py', *bg_args], start_new_session=True)
        bsdb = BackgroundStateDB()
        bg_jobs = [row[0] for row in bsdb.list_processes()]
        update_status(f'Running background jobs: {",".join(bg_jobs)}')
    return load_file(file, load_func, patient=patient)

rewards_expired = has_expired(nft_rewards_csv, 3600)
rewards = load_data(nft_rewards_csv, bg_args=['update_nft_rewards', nft_rewards_csv], force_update=rewards_expired, patient=True)

coin_prices_expired = True
coin_prices = load_data(coin_prices_csv, bg_args=['update_coin_prices', coin_prices_csv, 'ethereum', 'gala', 'town-star'], force_update=coin_prices_expired, patient=True)
coin_prices = coin_prices.set_index('coin')

with st.sidebar:
    item_count_text = st.empty()

    for line in [
        '## Coin prices \n',
        f'''
        | ETH | GALA | TOWN |
        | --- | --- | --- |
        | ${coin_prices.loc["ethereum"].usd:.1f} | ${coin_prices.loc["gala"].usd:.3f} | ${coin_prices.loc["town-star"].usd:.3f} |
        '''
        ]:
        st.markdown(line)

    st.write('\n')
    update_status('Idle')

    cols = st.columns([0.6, 0.4])
    with cols[0]:
        update_token_id_btn = st.button('Update Token IDs')
    with cols[1]:
        update_prices_btn = st.button('Update Prices')
    cols = st.columns([0.6, 0.4])
    with cols[0]:
        price_sort_map = {
            'Change': 'OS Change',
            'DTC': 'DTC',
            'Arb': 'Arb',
            'Price': 'OS USD',
            'Last Sale Price': 'OS LastSale USD',
            'GS Qty': 'GS Qty',
        }
        sort_option = st.selectbox(label='Sort By', options=price_sort_map.keys())
    with cols[1]:
        sort_order = st.selectbox(label='Order', options=['ASC', 'DESC'])

nft_prices_expired = update_token_id_btn or has_expired(nft_prices_csv, 60)
prices = load_data(nft_prices_csv, bg_args=['update_nft_prices', nft_prices_csv, nft_rewards_csv, coin_prices_csv, '1' if update_token_id_btn else ''], force_update=nft_prices_expired)

if prices is not None:
    def get_reward(name):
        name_match = (rewards.name == name)
        if not any(name_match):
            # Sometimes names don't match up exactly between data sources
            name_match = (lowertrim(rewards.name).str.startswith(lowertrim(name)))
        if not any(name_match):
            print(f'Warning: Reward info unavailable for {name}')
            return float('nan')
        r = rewards[name_match]
        n = len(r)
        if n > 1:
            print(f'Warning: {name} has {n} reward matches: {r}')
        return float(r.reward.iloc[0])

    prices['Reward'] = prices.Name.map(get_reward)
    prices['DTC'] = prices['OS USD'] / (prices.Reward * coin_prices.loc['town-star'].usd)

    item_count_text.write(f'Total: {len(prices)} items')

with st.sidebar:    
    arb_only = st.checkbox('Arb Only', value=False)
    max_eth = st.slider('Max ETH', min_value=0.1, max_value=10.0, value=1.0, step=0.01)
    dtc_warn_threshold = st.slider('DTC Alert Threshold', step=1, min_value=1, max_value=200, value=130)

# Generate markdown table
md_exclude_headers =  ['token_id', 'LastUpdate', 'OS Link', 'OS Qty', 'GS Link', 'GS Qty']
def generate_md_row(row):
    str_vals = []
    os_qty = f'{max(row["OS Qty"], 0):.0f}'
    if os_qty in ['0', 'nan']:
        os_qty = '⚠️'
    gs_qty = f'{max(row["GS Qty"], 0):.0f}'
    if gs_qty in ['0', 'nan']:
        gs_qty = '⚠️'
    for idx, val in zip(row.index, row.values):
        if idx in md_exclude_headers:
            continue
        str_vals.append(str({
            'OS ETH': lambda v: f'{float(v):.4f}',
            'OS USD': lambda v: f'${v:,.0f}',
            'OS LastSale USD': lambda v: f'${v:,.0f}',
            'GS USD': lambda v: f'${v:,.0f}',
            'OS Change': lambda v: f'{v:,.1f}%',
            'Reward': lambda v: f'{v:.0f} (${v * coin_prices.loc["town-star"].usd:.1f})',
            'DTC': lambda v: f'{v:.1f}',
            'Arb': lambda v: f'${v:,.0f}',
            'Name': lambda v: f'{v}<br/>([OS:{os_qty}]({row["OS Link"]}), [GS:{gs_qty}]({row["GS Link"]}))'
        }.get(idx, lambda v: v)(val)))
    return ('|' + '|'.join(str_vals) + '|')


def generate_md_header(cols):
    cols = [c for c in cols if c not in md_exclude_headers]
    return [
        ('|' + '|'.join(cols) + '|'),
        ('|' + '|'.join('---' for _ in cols) + '|')
    ]

search_text = st.text_input(label='Search Item: ', value='')

last_price_update_time = 'Never'
table_md = ''
if prices is not None:
    # Filter and sort prices
    last_price_update_time = datetime.fromisoformat(prices.LastUpdate.iloc[0]).strftime("%Y-%m-%d %H:%M")
    if arb_only:
        prices = prices[prices['Arb'] > 0]

    prices = prices[prices['OS ETH'] <= max_eth]\
        .sort_values(price_sort_map.get(sort_option, sort_option),
        ascending=
        {
            'ASC': True,
            'DESC': False
        }[sort_order])

    if search_text:
        prices = prices[lowertrim(prices['Name']).str.contains(lowertrim(search_text))]
        
    table_md = '\n'.join(generate_md_header(prices.columns) + prices.apply(generate_md_row, axis=1).values.tolist())

notif_text = st.empty()
f'### Last Price Update: {last_price_update_time}'
st.write(table_md, unsafe_allow_html=True)

def get_countdown():
    return ((page_last_refresh_time + timedelta(seconds=page_refresh_interval)) - datetime.now()).total_seconds()

with open('data/chime.wav', "rb") as f:
    audio_bytes = f.read()

audio_html = f"""
    <audio autoplay=True loop>
    <source src="data:audio/ogg;base64,{base64.b64encode(audio_bytes).decode()}" type="audio/ogg" autoplay=True>
    Your browser does not support the audio element.
    </audio>
"""
audio_widget = st.empty()
while get_countdown() <= page_refresh_interval:
    if prices is not None and len(prices[prices.DTC < dtc_warn_threshold]) > 0:
        notif_text.write(f'Check DTC < {dtc_warn_threshold} !!! Refresh in ~{round(get_countdown())}s')
        audio_widget.write(audio_html, unsafe_allow_html=True)
    else:
        notif_text.write(f'Will refresh in ~{round(get_countdown())}s')
        audio_widget.empty()
    sleep(1)
    notif_text.empty()