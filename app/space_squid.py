import os
from datetime import datetime, timedelta
import base64

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

# st.set_page_config(layout='wide')
from util import *

page_refresh_interval = 30  # seconds
refresh_count = st_autorefresh(interval=page_refresh_interval * 1000)
page_last_refresh_time = datetime.now()

'''# SpaceSquid
---'''

coin_prices = get_coin_price('ethereum', 'gala', 'town-star')
f'''### Coin prices
| ETH | GALA | TOWN |
|-----|------|------|
| ${coin_prices["ethereum"]:.1f} | ${coin_prices["gala"]:.3f} | ${coin_prices["town-star"]:.3f} |
---
'''

'''### Status'''
status_text = st.empty()
status_text.write('Idle')


rewards = fetch_rewards()
reward_item_names = rewards.NFT.str.lower().str.strip().values
def filter_asset_by_name(a):
    # Filter by item name
    if 'name' not in a or\
        not isinstance(a['name'], str) or\
        a['name'].lower().strip() not in reward_item_names:
        return False
    return True

def update_token_ids():
    assets = list(fetch_items(status_text, attribute_filter=filter_asset_by_name))
    f'{len(assets)} items fetched'
    return [a['token_id'] for a in assets], assets


token_id_csv = 'data/town_star_token_ids.csv'
token_ids = pd.read_csv(token_id_csv) if os.path.exists(token_id_csv) else None
assets = None
if token_ids is None or st.button('Update Token IDs'):
    token_ids, assets = update_token_ids()
    token_ids = pd.DataFrame(token_ids, columns=['token_id'])
    token_ids.to_csv(token_id_csv, index=False)
    status_text.write(f'Token ID list updated (n={len(token_ids)}); assets fetched')

with st.expander('Token IDs'):
    token_ids

def filter_asset_by_token_id(a):
    return 'token_id' in a and a['token_id'] in token_ids

prices_csv = 'data/town_star_prices.csv'
prices = pd.read_csv(prices_csv) if os.path.exists(prices_csv) else None
last_price_refresh = datetime.fromisoformat(prices['LastUpdate'].iloc[0])
expired = (datetime.now() - last_price_refresh).total_seconds() > page_refresh_interval
if prices is None or st.button('Update Prices') or expired:
    status_text.write('Updating prices...')
    assets = fetch_items(token_ids=token_ids.token_id)
    prices = fetch_prices(assets)
    prices['Reward'] = prices.Name.map(lambda n: float(rewards[rewards.NFT == n]['TOWN Value'].iloc[0]))
    prices['ROI'] = prices.USD / (prices.Reward * coin_prices['town-star'])
    prices['LastUpdate'] = datetime.now().isoformat()
    prices.to_csv(prices_csv, index=False)
    status_text.empty()

prices = prices.sort_values(['ROI'])

# Generate markdown table instead of
# st.dataframe(prices.style.format(subset=['Reward', 'ROI', 'USD', 'Qty'], formatter="{:.0f}").format(subset=['ETH'], formatter="{:.3f}"))
def generate_md_row(row):
    str_vals = []
    for idx, val in zip(row.index, row.values):
        if idx == 'LastUpdate':
            continue
        str_vals.append(str({
            'Link': lambda v: f'[OpenSea]({v})',
            'ETH': lambda v: f'{float(v):.4f}',
            'USD': lambda v: f'${v:,.0f}',
            'Qty': lambda v: f'{v:.0f}',
            'Reward': lambda v: f'{v:.0f} (${v * coin_prices["town-star"]:.1f})',
            'ROI': lambda v: f'{v:.1f}',
        }.get(idx, lambda v: v)(val)))
    return ('|' + '|'.join(str_vals) + '|')


def generate_md_header(cols):
    cols = [c for c in cols if c != 'LastUpdate']
    return [
        ('|' + '|'.join(cols) + '|'),
        ('|' + '|'.join('---' for _ in cols) + '|')
    ]

max_eth = st.slider('Max ETH', min_value=0.1, max_value=10.0, value=0.8, step=0.01)
notif_text = st.empty()
roi_warn_threshold = st.slider('ROI threshold', step=1, min_value=1, max_value=150, value=110)
f'### Last Update: {datetime.fromisoformat(prices.LastUpdate.iloc[0]).strftime("%Y-%m-%d %H:%M")}'
md = '\n'.join(generate_md_header(prices.columns) + prices[prices.ETH <= max_eth].apply(generate_md_row, axis=1).values.tolist())
st.markdown(md)

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
    if len(prices[prices.ROI < roi_warn_threshold]) > 0:
        notif_text.write(f'Check ROI < {roi_warn_threshold} !!! Refresh in ~{round(get_countdown())}s')
        audio_widget.markdown(audio_html, unsafe_allow_html=True)
    else:
        notif_text.write(f'Will refresh in ~{round(get_countdown())}s')
        audio_widget.empty()
    sleep(1)
    notif_text.empty()