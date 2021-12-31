import os
from datetime import datetime, timedelta
import base64
from numpy.core.fromnumeric import sort

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from util import *

st.set_page_config(page_title='SpaceSquid')

page_refresh_interval = 60  # seconds
refresh_count = st_autorefresh(interval=page_refresh_interval * 1000)
page_last_refresh_time = datetime.now()

rewards = fetch_rewards()
reward_item_names = lowertrim(rewards.name).values
def filter_asset_by_name(a):
    # Filter by item name
    if 'name' in a and\
        isinstance(a['name'], str) and \
        any(lowertrim(a['name']) in n for n in reward_item_names):
        return True
    return False

coin_prices = fetch_coin_prices('ethereum', 'gala', 'town-star')

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
    item_count_text = st.empty()
    def update_status(s):
        status_text.write('## Status: ' + s)

    for line in [
        '## Coin prices \n',
        f'''
        | ETH | GALA | TOWN |
        | --- | --- | --- |
        | ${coin_prices["ethereum"]:.1f} | ${coin_prices["gala"]:.3f} | ${coin_prices["town-star"]:.3f} |
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
            'GS Qty': 'GS Qty'
        }
        sort_option = st.selectbox(label='Sort By', options=price_sort_map.keys())
    with cols[1]:
        sort_order = st.selectbox(label='Order', options=['ASC', 'DESC'])


def update_token_ids():
    assets = list(fetch_items(update_status, attribute_filter=filter_asset_by_name))
    return [a['token_id'] for a in assets], assets


token_id_csv = 'data/town_star_token_ids.csv'
token_ids = pd.read_csv(token_id_csv) if os.path.exists(token_id_csv) else None
assets = None
if token_ids is None or update_token_id_btn:
    token_ids, assets = update_token_ids()
    token_ids = pd.DataFrame(token_ids, columns=['token_id'])
    token_ids.to_csv(token_id_csv, index=False)
    update_status(f'Token ID list updated (n={len(token_ids)}); assets fetched')

def filter_asset_by_token_id(a):
    return 'token_id' in a and a['token_id'] in token_ids

prices_csv = 'data/town_star_prices.csv'
prices = pd.read_csv(prices_csv) if os.path.exists(prices_csv) else None
last_price_refresh = datetime.now() - timedelta(seconds=page_refresh_interval*2) \
    if prices is None else datetime.fromisoformat(prices['LastUpdate'].iloc[0])
expired = (datetime.now() - last_price_refresh).total_seconds() > page_refresh_interval
if prices is None or update_prices_btn or expired:
    if update_prices_btn:
        print('Update price button pressed')
    if expired:
        print('Prices expired')
    update_status('Updating prices...')
    assets = list(fetch_items(token_ids=token_ids.token_id))
    prices = parse_prices(assets)

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
    prices['DTC'] = prices['OS USD'] / (prices.Reward * coin_prices['town-star'])
    prices['LastUpdate'] = datetime.now().isoformat()
    prices.to_csv(prices_csv, index=False)
    update_status('Done')

item_count_text.write(f'Total: {len(prices)} items')

with st.sidebar:    
    arb_only = st.checkbox('Arb Only', value=False)
    max_eth = st.slider('Max ETH', min_value=0.1, max_value=10.0, value=1.0, step=0.01)
    dtc_warn_threshold = st.slider('DTC Alert Threshold', step=1, min_value=1, max_value=200, value=130)

# Generate markdown table
md_exclude_headers =  ['LastUpdate', 'OS Link', 'OS Qty', 'GS Link', 'GS Qty']
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
            'GS USD': lambda v: f'${v:,.0f}',
            'OS Change': lambda v: f'{v:,.1f}%',
            'Reward': lambda v: f'{v:.0f} (${v * coin_prices["town-star"]:.1f})',
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

# Filter and sort prices
last_update_time = prices.LastUpdate.iloc[0]
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

notif_text = st.empty()
f'### Last Update: {datetime.fromisoformat(last_update_time).strftime("%Y-%m-%d %H:%M")}'
md = '\n'.join(generate_md_header(prices.columns) + prices.apply(generate_md_row, axis=1).values.tolist())
st.write(md, unsafe_allow_html=True)

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
    if len(prices[prices.DTC < dtc_warn_threshold]) > 0:
        notif_text.write(f'Check DTC < {dtc_warn_threshold} !!! Refresh in ~{round(get_countdown())}s')
        audio_widget.write(audio_html, unsafe_allow_html=True)
    else:
        notif_text.write(f'Will refresh in ~{round(get_countdown())}s')
        audio_widget.empty()
    sleep(1)
    notif_text.empty()