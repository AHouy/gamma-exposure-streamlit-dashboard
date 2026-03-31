import streamlit as st
from streamlit_highcharts import streamlit_highcharts
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from streamlit_gsheets import GSheetsConnection
import urllib.parse
import time
import os
import threading

st.set_page_config(page_title="Gamma Exposure Dashboard", layout="wide")

st.title("Gamma Exposure Dashboard")

# Initialize session state for live data and animation
if 'is_playing' not in st.session_state:
    st.session_state.is_playing = False
if 'rewind_idx' not in st.session_state:
    st.session_state.rewind_idx = -1
if 'normalized_heatmap' not in st.session_state:
    st.session_state.normalized_heatmap = True


# --- Background Data Coordinator ---
@st.cache_resource
class DataCoordinator:
    def __init__(self):
        self.full_df = pd.DataFrame()
        self.last_row_count = 0
        self.last_sync_timestamp: float = 0.0
        self.is_syncing = False
        self.lock = threading.Lock()
        self.error: str = ""

    def fetch_raw(self, offset):
        """Standard incremental fetch logic from Gviz."""
        SHEET_ID = "1oe96VFlcWbeEMMKrMjxEroOxbH0wFG6LlCny1gJzc8Q"
        SHEET_GID = "1881582747"
        if offset > 0:
            query = urllib.parse.quote(f"SELECT * OFFSET {offset}")
            url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&gid={SHEET_GID}&tq={query}"
        else:
            url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&gid={SHEET_GID}"
        
        # print(f"Background Fetch: {url}") # Log for debugging
        return pd.read_csv(url)

    def sync(self, force_sync=False):
        """Triggers a background sync task."""
        if self.is_syncing:
            return
        
        # Ensure we don't hit Google too hard (60s throttle)
        if not force_sync and (time.time() - self.last_sync_timestamp < 60):
            return

        def _sync_task():
            self.is_syncing = True
            try:
                new_data_raw = self.fetch_raw(self.last_row_count)
                if not new_data_raw.empty:
                    raw_count = len(new_data_raw)
                    new_df = new_data_raw.copy()
                    
                    # Standardize new rows
                    new_df['timestamp'] = pd.to_datetime(new_df['timestamp'])
                    new_df['timestamp'] = new_df['timestamp'].dt.floor('1s')
                    new_df['date'] = new_df['timestamp'].dt.date
                    
                    numeric_cols = [col for col in new_df.columns if col not in ['timestamp', 'ticker', 'date']]
                    for col in numeric_cols:
                        new_df[col] = pd.to_numeric(new_df[col], errors='coerce').fillna(0)
                        
                    if 'dealer_delta_oi' in new_df.columns:
                        new_df['dealer_delta_oi'] = new_df['dealer_delta_oi'] / 100.0
                        
                    new_df = new_df.dropna(subset=['timestamp', 'strike', 'price'])
                    
                    with self.lock:
                        if self.full_df.empty:
                            self.full_df = new_df
                        else:
                            self.full_df = pd.concat([self.full_df, new_df], ignore_index=True)
                            self.full_df = self.full_df.drop_duplicates(subset=['timestamp', 'ticker', 'strike'])
                        
                        self.last_row_count += raw_count
                        self.last_sync_timestamp = time.time()
                else:
                    self.last_sync_timestamp = time.time() # Even if empty, mark as synced
            except Exception as e:
                self.error = str(e)
            finally:
                self.is_syncing = False

        threading.Thread(target=_sync_task, daemon=True).start()

    def get_data(self):
        with self.lock:
            return self.full_df.copy(), self.last_sync_timestamp

# Helper to get singleton
coordinator = DataCoordinator()

def load_full_data(force_sync=False):
    """Bridge to the background coordinator."""
    if force_sync:
        # For full reset, we clear and re-init
        coordinator.full_df = pd.DataFrame()
        coordinator.last_row_count = 0
        coordinator.last_sync_timestamp = 0
        
    coordinator.sync(force_sync=force_sync)
    df, _ = coordinator.get_data()
    return df

# --- Data Sync Controls ---


# --- Background Sync Fragment ---
if 'last_ui_sync' not in st.session_state:
    st.session_state.last_ui_sync = 0.0

@st.fragment(run_every=10)
def background_monitor():
    # If the coordinator has new data since our last UI render, trigger rerun
    _, last_sync = coordinator.get_data()
    st.caption(f"Last Synced: {time.strftime('%H:%M:%S', time.localtime(last_sync))}")
    
    if last_sync > st.session_state.last_ui_sync:
        st.session_state.last_ui_sync = last_sync
        st.rerun()
    
    # If auto-refresh is on and it's time to sync, trigger the background task
    if st.session_state.get('auto_refresh_enabled', True):
        coordinator.sync()

background_monitor()


# --- App Logic ---
# The database is now clean and typed. We keep light sanitization in fragments 
# only to handle live API surprises and ensure PyArrow schema stability.

# Ensure at least an initial load if state is empty
full_df, _ = coordinator.get_data()
if full_df.empty:
    # Synchronous initial fetch for first-run
    SHEET_ID = "1oe96VFlcWbeEMMKrMjxEroOxbH0wFG6LlCny1gJzc8Q"
    SHEET_GID = "1881582747"
    try:
        url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&gid={SHEET_GID}"
        initial_df = pd.read_csv(url)
        # We don't do full processing here to keep it simple, 
        # just enough to get started or just call the sync task directly
        # but synchronously.
        # Actually, let's just make the coordinator's sync support sync mode?
        # Or just use the existing logic for the first time.
        full_df = load_full_data()
    except:
        full_df = pd.DataFrame()

def get_tickers():
    if full_df.empty:
        return []
    return sorted(full_df['ticker'].unique().tolist())

def get_available_dates(ticker):
    if full_df.empty:
        return []
    ticker_data = full_df[full_df['ticker'] == ticker]
    return sorted(list(set(ticker_data['date'])), reverse=True)

def load_data(ticker, date=None):
    if full_df.empty:
        return pd.DataFrame()
    
    mask = (full_df['ticker'] == ticker)
    if date:
        mask = mask & (full_df['date'] == date)
        
    return full_df[mask].copy()
# ----------------- Data Loading -----------------
tickers = get_tickers()
if not tickers:
    st.warning("No tickers found in the database.")
    st.stop()
    
# Sort alphabetically but keep QQQ and SPY at the top
tickers = sorted(tickers)
priority = ['QQQ', 'SPY']
tickers = [t for t in priority if t in tickers] + [t for t in tickers if t not in priority]

# ----------------- Timeframe Controls -----------------
st.sidebar.header("Filters")
selected_ticker = st.sidebar.selectbox("Select Ticker", tickers, index=tickers.index('QQQ') if 'QQQ' in tickers else 0)

# Store current ticker in session state for the fragment
st.session_state.current_ticker = selected_ticker

# Stored ticker for later use
st.session_state.current_ticker = selected_ticker

# --- Toggles and Timeframe (Moved UP) ---
st.sidebar.markdown("---")
timeframe = st.sidebar.selectbox("Timeframe", ["Last 15 Minutes", "Last 30 Minutes", "Last 1 Hour", "Last 4 Hours", "Full Day"], index=4)
total_strikes = st.sidebar.slider("Total Number of Strikes", min_value=1, max_value=50, value=20)
st.session_state.normalized_heatmap = st.sidebar.checkbox("Normalized Heatmap", value=st.session_state.normalized_heatmap)

# Aligning with index.html color scheme
GAMMA_METRICS = {
    'Open Interest': 'dealer_gamma_oi',
    'Net Volume': 'dealer_gamma_vol',
    'Call Vol': 'call_gamma_vol',
    'Put Vol': 'put_gamma_vol'
}

DELTA_METRICS = {
    'Open Interest': 'dealer_delta_oi',
    'Net Volume': 'dealer_delta_vol',
    'Call Vol': 'call_delta_vol',
    'Put Vol': 'put_delta_vol'
}

CHARM_METRICS = {
    'Open Interest': 'dealer_charm_oi',
    'Net Volume': 'dealer_charm_vol',
    'Call Vol': 'call_charm_vol',
    'Put Vol': 'put_charm_vol'
}

LABEL_MAP = {
    'dealer_gamma_oi': 'Open Interest',
    'dealer_gamma_vol': 'Net Volume',
    'call_gamma_vol': 'Call Vol',
    'put_gamma_vol': 'Put Vol',
    'dealer_delta_oi': 'Open Interest',
    'dealer_delta_vol': 'Net Volume',
    'call_delta_vol': 'Call Vol',
    'put_delta_vol': 'Put Vol',
    'dealer_charm_oi': 'Open Interest',
    'dealer_charm_vol': 'Net Volume',
    'call_charm_vol': 'Call Vol',
    'put_charm_vol': 'Put Vol'
}



# --- Rewind / Replay Block (Moved DOWN) ---
st.sidebar.markdown("---")
st.sidebar.subheader("🕒 Rewind / Replay")
is_rewind = st.sidebar.checkbox("Enable Rewind", value=False)

selected_date = None
if is_rewind:
    available_dates = get_available_dates(selected_ticker)
    if available_dates:
        # Default to the most recent date
        selected_date = st.sidebar.selectbox(
            "Select Date", 
            available_dates,
            index=0
        )
    else:
        st.sidebar.warning(f"No historical dates found for {selected_ticker}")

df = load_data(selected_ticker, date=selected_date)

def get_processed_ticker_data(ticker, df):
    if df.empty:
        return pd.DataFrame()
    
    ticker_df = df[df['ticker'] == ticker].copy()
    if ticker_df.empty:
        return pd.DataFrame()

    # Strictly ensure unique strikes per timestamp to avoid chart overlap/flicker
    ticker_df = ticker_df.sort_values(['timestamp', 'strike'])
    ticker_df = ticker_df.drop_duplicates(subset=['timestamp', 'strike'], keep='last')
    return ticker_df

cache_key = f"processed_df_{selected_ticker}"
last_full_len = len(full_df)

if cache_key not in st.session_state or st.session_state.get('last_full_len', 0) != last_full_len:
    st.session_state[cache_key] = get_processed_ticker_data(selected_ticker, full_df)
    st.session_state.last_full_len = last_full_len

ticker_df = st.session_state[cache_key]
if ticker_df.empty:
    st.warning(f"No data available for ticker: {selected_ticker}")
    st.stop()

latest_timestamp = ticker_df['timestamp'].max()
available_dates_in_df = sorted(list(set(df['timestamp'].dt.date)))

current_date_obj = None
if is_rewind and selected_date:
    try:
        current_date_obj = pd.to_datetime(selected_date).date()
    except:
        current_date_obj = None

if 'last_ticker' not in st.session_state:
    st.session_state.last_ticker = selected_ticker
if 'last_date' not in st.session_state:
    st.session_state.last_date = selected_date

ticker_changed = st.session_state.last_ticker != selected_ticker
date_changed = is_rewind and (st.session_state.last_date != selected_date)

if is_rewind and available_dates_in_df:
    ts_series = ticker_df[ticker_df['date'] == current_date_obj]['timestamp']
    unique_timestamps = sorted([ts for ts in ts_series.unique() if pd.notnull(ts)])
else:
    unique_timestamps = sorted([ts for ts in ticker_df['timestamp'].unique() if pd.notnull(ts)])

ts_cache_key = f"unique_ts_{selected_ticker}_{selected_date}"
if ts_cache_key not in st.session_state or ticker_changed or date_changed:
    st.session_state[ts_cache_key] = unique_timestamps
else:
    unique_timestamps = st.session_state[ts_cache_key]

if ticker_changed or date_changed:
    if unique_timestamps:
        st.session_state.rewind_idx = 0 
        st.session_state.rewind_slider = unique_timestamps[0]
    else:
        st.session_state.rewind_idx = -1
    st.session_state.last_ticker = selected_ticker
    st.session_state.last_date = selected_date
    st.session_state.is_playing = False
    st.toast(f"Switched context to {selected_ticker}" if ticker_changed else f"Date changed to {selected_date}")

if st.session_state.get('is_playing', False) and len(unique_timestamps) > 0:
    if st.session_state.rewind_idx < len(unique_timestamps) - 1:
        st.session_state.rewind_idx += 1
        st.session_state.rewind_slider = unique_timestamps[st.session_state.rewind_idx]
    else:
        st.session_state.is_playing = False
        st.toast("Reached end of historical data")

if is_rewind and len(unique_timestamps) > 1:
    ctrl_cols = st.sidebar.columns([1, 1.2, 1])
    if ctrl_cols[0].button("⏪", use_container_width=True):
        st.session_state.is_playing = False
        if st.session_state.rewind_idx > 0:
            st.session_state.rewind_idx -= 1
            st.session_state.rewind_slider = unique_timestamps[st.session_state.rewind_idx]
            st.rerun()
    if st.session_state.get('is_playing', False):
        if ctrl_cols[1].button("⏹️ Stop", use_container_width=True):
            st.session_state.is_playing = False
            st.rerun()
    else:
        if ctrl_cols[1].button("▶️ Play", use_container_width=True):
            if st.session_state.rewind_idx >= len(unique_timestamps) - 1:
                st.session_state.rewind_idx = 0
            st.session_state.is_playing = True
            st.rerun()
    if ctrl_cols[2].button("⏩", use_container_width=True):
        st.session_state.is_playing = False
        if st.session_state.rewind_idx < len(unique_timestamps) - 1:
            st.session_state.rewind_idx += 1
            st.session_state.rewind_slider = unique_timestamps[st.session_state.rewind_idx]
            st.rerun()

    if st.session_state.rewind_idx == -1 or st.session_state.rewind_idx >= len(unique_timestamps):
        st.session_state.rewind_idx = len(unique_timestamps) - 1
        st.session_state.rewind_slider = unique_timestamps[st.session_state.rewind_idx]

    selected_time = st.sidebar.select_slider(
        "Select Snapshot Time",
        options=unique_timestamps,
        format_func=lambda x: x.strftime("%H:%M:%S"),
        key="rewind_slider"
    )
    try:
        st.session_state.rewind_idx = unique_timestamps.index(st.session_state.rewind_slider)
    except ValueError:
        st.session_state.rewind_idx = len(unique_timestamps) - 1
        st.session_state.rewind_slider = unique_timestamps[st.session_state.rewind_idx]
    view_timestamp = st.session_state.rewind_slider
    st.sidebar.info(f"Viewing data from: {view_timestamp.strftime('%H:%M:%S')}")
else:
    view_timestamp = latest_timestamp
    st.session_state.is_playing = False
    if is_rewind and len(unique_timestamps) <= 1:
        st.sidebar.warning(f"Not enough historical data for {selected_date} to rewind.")

# --- Filtering and Final Data Setup ---
ticker_df = ticker_df[ticker_df['timestamp'] <= view_timestamp]
if timeframe != "Full Day":
    minutes_map = {"Last 15 Minutes": 15, "Last 30 Minutes": 30, "Last 1 Hour": 60, "Last 4 Hours": 240}
    cutoff_time = view_timestamp - pd.Timedelta(minutes=minutes_map[timeframe])
    ticker_df = ticker_df[ticker_df['timestamp'] >= cutoff_time]
else:
    # If Full Day, filter to the specific date of the view_timestamp
    ticker_df = ticker_df[ticker_df['date'] == view_timestamp.date()]

snapshot_df = ticker_df[ticker_df['timestamp'] == view_timestamp]
if snapshot_df.empty:
    snapshot_df = df[df['timestamp'] == view_timestamp]

if snapshot_df.empty:
    st.warning(f"No snapshot data available for {selected_ticker} at {view_timestamp}")
    st.stop()

if not snapshot_df.empty and pd.notnull(snapshot_df['price'].iloc[0]):
    current_price = float(snapshot_df['price'].iloc[0])
elif not ticker_df.empty:
    valid_prices = ticker_df['price'].dropna()
    current_price = float(valid_prices.iloc[-1]) if not valid_prices.empty else 100.0
else:
    current_price = 100.0

COLOR_PALETTE = {
    'dealer_gamma_oi': '#8b5cf6',   # Purple
    'dealer_gamma_vol': '#f59e0b',  # Gold
    'call_gamma_vol': '#10b981',    # Green
    'put_gamma_vol': '#ef4444',     # Red
    'dealer_delta_oi': '#8b5cf6',
    'dealer_delta_vol': '#f59e0b',
    'call_delta_vol': '#10b981',
    'put_delta_vol': '#ef4444',
    'dealer_charm_oi': '#8b5cf6',
    'dealer_charm_vol': '#f59e0b',
    'call_charm_vol': '#10b981',
    'put_charm_vol': '#ef4444'
}

HISTORICAL_MARKERS = {
    1: 'circle',
    5: 'diamond',
    10: 'square',
    15: 'triangle',
    30: 'triangle-down'
}

LABEL_MAP = {
    'dealer_gamma_oi': 'Open Interest',
    'dealer_gamma_vol': 'Net Volume',
    'call_gamma_vol': 'Call Vol',
    'put_gamma_vol': 'Put Vol',
    'dealer_delta_oi': 'Open Interest',
    'dealer_delta_vol': 'Net Volume',
    'call_delta_vol': 'Call Vol',
    'put_delta_vol': 'Put Vol',
    'dealer_charm_oi': 'Open Interest',
    'dealer_charm_vol': 'Net Volume',
    'call_charm_vol': 'Call Vol',
    'put_charm_vol': 'Put Vol'
}





# Isolate strikes around the closest strike
if snapshot_df.empty:
    st.warning(f"No snapshot data available for {selected_ticker}.")
    st.stop()

# Find the closest strike to the current price efficiently
unique_strikes = sorted(snapshot_df['strike'].unique().tolist())
if not unique_strikes:
    st.warning(f"No strikes found for {selected_ticker}.")
    st.stop()

# Find the index of the strike closest to current_price
closest_idx = min(range(len(unique_strikes)), key=lambda i: abs(float(unique_strikes[i]) - current_price))

# Select the range of strikes to display
# Calculate spread for total strikes (centered)
half_spread = total_strikes // 2
start_index = max(0, closest_idx - half_spread)
end_index = min(len(unique_strikes) - 1, start_index + total_strikes - 1)
# Re-adjust start if end hit the boundary
if end_index - start_index < total_strikes - 1:
    start_index = max(0, end_index - total_strikes + 1)
valid_strikes = unique_strikes[int(start_index):int(end_index) + 1]

filtered_snapshot = snapshot_df[snapshot_df['strike'].isin(valid_strikes)].sort_values(by='strike', ascending=False)
filtered_timeseries = ticker_df[ticker_df['strike'].isin(valid_strikes)]

# To ensure "1m ago, 5m ago" works correctly during rewind, 
# we use the full combined context (including live data)
# but we limit it to the current selected view_timestamp and valid strikes
cached_combined = st.session_state[cache_key]
historical_context_df = cached_combined[(cached_combined['timestamp'] <= view_timestamp) & (cached_combined['strike'].isin(valid_strikes))]

def create_highcharts_bar(data, title, value_vars, label_map, current_price, timeseries_data=None, height=800):
    if data.empty:
        return None

    # Sort strikes and prepare labels
    data = data.sort_values('strike', ascending=True)
    categories = data['strike'].map(lambda x: f"${x:,.2f}").tolist()
    
    series = []
    
    # Add PV, CV, NV, OI to get OI at the TOP
    target_vars = value_vars[::-1] 
    
    for i, var in enumerate(target_vars):
        label = label_map[var]
        color = COLOR_PALETTE.get(var, '#ffffff')
        
        # Base Bar series
        series.append({
            "name": label,
            "data": data[var].tolist(),
            "type": "bar",
            "color": color,
            "grouping": True,
            "zIndex": 1
        })
        # Remove scatter points for a cleaner look
        pass

    options = {
        "chart": { 
            "type": "bar", 
            "height": height, 
            "backgroundColor": "transparent", 
            "animation": False,
            "spacingTop": 40,
            "spacingBottom": 40,
            "spacingLeft": 10,
            "spacingRight": 20,
            "style": { "fontFamily": "'Inter', sans-serif" }
        },
        "title": { 
            "text": title, 
            "align": "left",
            "style": { "color": "#e2e8f0", "fontSize": "18px", "fontWeight": "bold" } 
        },
        "xAxis": { 
            "categories": categories, 
            "title": { "text": "Strike", "style": { "color": "#94a3b8" } }, 
            "labels": { "style": { "color": "#94a3b8" } }, 
            "gridLineColor": "rgba(255, 255, 255, 0.05)",
            "reversed": False 
        },
        "yAxis": { 
            "title": { "text": "Exposure", "style": { "color": "#94a3b8" } }, 
            "gridLineColor": "rgba(255, 255, 255, 0.05)", 
            "labels": { "style": { "color": "#94a3b8" } } 
        },
        "legend": { 
            "itemStyle": { "color": "#e2e8f0" }, 
            "itemHoverStyle": { "color": "#ffffff" },
            "verticalAlign": "top", 
            "align": "center", 
            "reversed": True,
            "y": 10 
        },
        "tooltip": {
            "backgroundColor": "rgba(15, 17, 26, 0.9)",
            "style": { "color": "#e2e8f0" },
            "borderColor": "rgba(255, 255, 255, 0.1)",
            "borderRadius": 8
        },
        "plotOptions": { 
            "bar": { "dataLabels": { "enabled": False } }, 
            "series": { "animation": False } 
        },
        "series": series,
        "credits": { "enabled": False }
    }
    
    # Add Current Price Plot Line
    # Find category index closest to current_price
    strikes = data['strike'].tolist()
    closest_idx = min(range(len(strikes)), key=lambda i: abs(strikes[i] - current_price))
    
    options["xAxis"]["plotLines"] = [{
        "color": "violet",
        "value": closest_idx,
        "width": 2,
        "dashStyle": "Dash",
        "zIndex": 5,
        "label": { "text": f"Price: ${current_price:,.2f}", "style": { "color": "violet", "fontWeight": "bold" }, "align": "right", "x": -10 }
    }]
    
    return options


def create_highcharts_heatmap(df, title, metric, current_price, is_normalized=True):
    if df.empty:
        return None

    # Get unique strikes and timestamps
    df = df.copy()
    df['timestamp_ms'] = df['timestamp'].view('int64') // 10**6
    
    strikes = sorted(df['strike'].unique().tolist())
    # Sort descending for the heatmap y-axis (categories)
    strikes_desc = sorted(strikes, reverse=True)
    strike_to_idx = {s: i for i, s in enumerate(strikes_desc)}
    categories = [f"${s:,.2f}" for s in strikes_desc]

    # Prepare heatmap data: [x, y, value]
    heatmap_data = []
    price_data = []
    
    # Group by timestamp for normalization if needed
    for ts_ms, group in df.groupby('timestamp_ms'):
        # Price for this slice
        slice_price = group['price'].iloc[0] if 'price' in group.columns else current_price
        
        # Calculate Y position for price line (interpolated between categories)
        y_val = None
        if len(strikes_desc) > 1:
            for i in range(len(strikes_desc) - 1):
                # Since strikes are descending, s_high is index i, s_low is index i+1
                s_high = strikes_desc[i]
                s_low = strikes_desc[i+1]
                if s_low <= slice_price <= s_high:
                    y_val = i + (s_high - slice_price) / (s_high - s_low)
                    break
        price_data.append({"x": int(ts_ms), "y": y_val, "price": slice_price})

        # Local normalization limit
        if is_normalized:
            local_max = group[metric].abs().max()
            if local_max == 0: local_max = 1
        else:
            local_max = 1

        for _, row in group.iterrows():
            raw_val = row[metric]
            val = raw_val / local_max if is_normalized else raw_val
            heatmap_data.append([int(ts_ms), strike_to_idx[row['strike']], val])

    color_min = -1 if is_normalized else df[metric].min()
    color_max = 1 if is_normalized else df[metric].max()

    options = {
        "chart": { 
            "type": 'heatmap', 
            "backgroundColor": 'transparent', 
            "height": 600, 
            "animation": False 
        },
        "plotOptions": {
            "series": { "animation": False }
        },
        "title": { "text": title, "align": "left", "style": { "color": "#f8fafc", "fontWeight": "bold" } },
        "xAxis": {
            "type": 'datetime',
            "title": { "text": 'Time', "style": { "color": "#94a3b8" } },
            "labels": { "style": { "color": "#94a3b8" } },
            "gridLineColor": 'rgba(255, 255, 255, 0.05)'
        },
        "yAxis": {
            "categories": categories,
            "title": { "text": 'Strike', "style": { "color": "#94a3b8" } },
            "labels": { "style": { "color": "#94a3b8" } },
            "gridLineColor": 'rgba(255, 255, 255, 0.05)',
            "reversed": True # Reversed so index 0 (Top strike) is at the TOP
        },
        "colorAxis": {
            "stops": [
                [0, '#ef4444'],    # Negative bound
                [0.5, 'rgba(30, 32, 47, 0)'], # Neutral
                [1, '#10b981']     # Positive bound
            ],
            "min": color_min,
            "max": color_max,
            "labels": { "style": { "color": "#94a3b8" } }
        },
        "legend": {
            "align": 'right', "layout": 'vertical', "margin": 0,
            "verticalAlign": 'middle', "symbolHeight": 280,
            "itemStyle": { "color": "#e2e8f0" }
        },
        "tooltip": {
            "backgroundColor": "rgba(15, 17, 26, 0.9)",
            "style": { "color": "#e2e8f0" },
            "borderColor": "rgba(255, 255, 255, 0.1)",
            "borderRadius": 8
        },
        "series": [{
            "name": metric,
            "type": 'heatmap',
            "data": heatmap_data,
            "colsize": 60000, # 1 minute
            "tooltip": { "pointFormat": 'Time: {point.x:%H:%M:%S}<br/>Strike: {point.y_category}<br/>Value: {point.value:,.2f}' }
        }, {
            "name": 'Price',
            "type": 'line',
            "data": price_data,
            "color": '#a952ff',
            "lineWidth": 3,
            "marker": { "enabled": False },
            "zIndex": 2,
            "tooltip": { "pointFormat": '<b>Price:</b> ${point.price:,.2f}' }
        }],
        "credits": { "enabled": False }
    }
    return options

def create_highcharts_line(df, title, metric):
    if df.empty:
        return None
    
    unique_strikes = sorted(df['strike'].unique().tolist(), reverse=True)
    series = []
    for strike in unique_strikes:
        strike_df = df[df['strike'] == strike].sort_values('timestamp')
        series.append({
            "name": f"${strike:,.2f}",
            "data": [[int(ts.timestamp() * 1000), val] for ts, val in zip(strike_df['timestamp'], strike_df[metric])],
        })

    options = {
        "chart": { 
            "type": 'line', 
            "backgroundColor": 'transparent', 
            "height": 600, 
            "animation": False 
        },
        "plotOptions": {
            "series": { "animation": False }
        },
        "title": { "text": title, "align": "left", "style": { "color": "#f8fafc", "fontWeight": "bold" } },
        "xAxis": {
            "type": 'datetime',
            "title": { "text": 'Time', "style": { "color": "#94a3b8" } },
            "labels": { "style": { "color": "#94a3b8" } },
            "gridLineColor": 'rgba(255, 255, 255, 0.05)'
        },
        "yAxis": {
            "title": { "text": 'Value', "style": { "color": "#94a3b8" } },
            "labels": { "style": { "color": "#94a3b8" } },
            "gridLineColor": 'rgba(255, 255, 255, 0.05)'
        },
        "legend": {
            "itemStyle": { "color": "#e2e8f0" },
            "layout": 'horizontal',
            "align": 'center',
            "verticalAlign": 'bottom'
        },
        "series": series,
        "credits": { "enabled": False }
    }
    return options

# --- Live Stats Bar ---
s_col1, s_col2, s_col3, s_col4, s_col5 = st.columns(5)
with s_col1:
    st.metric("Current Price", f"${current_price:.2f}")
with s_col2:
    # Calculate max gamma strike
    max_idx = filtered_snapshot['dealer_gamma_vol'].abs().idxmax() if not filtered_snapshot.empty else None
    max_g_strike = filtered_snapshot.loc[max_idx, 'strike'] if max_idx is not None else 0
    st.metric("Max Gamma Strike", f"${max_g_strike:.2f}")
with s_col3:
    total_delta = filtered_snapshot['dealer_delta_vol'].sum() if not filtered_snapshot.empty else 0
    st.metric("Total Delta Exposure", f"${total_delta:,.0f}")
with s_col4:
    st.metric("Latest Data", latest_timestamp.strftime('%H:%M:%S') if pd.notnull(latest_timestamp) else "N/A")
with s_col5:
    st.metric("Ticker", selected_ticker)

# ----------------- Render Main Dashboard -----------------
tab1, tab2 = st.tabs(["📊 Spot", "📈 Historical"])

with tab1:

    col1, col2 = st.columns(2)
    with col1:
        gamma_opts = create_highcharts_bar(
            filtered_snapshot, 
            f"Spot Gamma - {selected_ticker}", 
            list(GAMMA_METRICS.values()), 
            LABEL_MAP,
            current_price,
            timeseries_data=historical_context_df,
            height=1000
        )
        if gamma_opts:
            streamlit_highcharts(gamma_opts, key="spot_gamma", height=1000)

    with col2:
        delta_opts = create_highcharts_bar(
            filtered_snapshot, 
            f"Spot Delta - {selected_ticker}", 
            list(DELTA_METRICS.values()), 
            LABEL_MAP,
            current_price,
            timeseries_data=historical_context_df,
            height=1000
        )
        if delta_opts:
            streamlit_highcharts(delta_opts, key="spot_delta", height=1000)

    col3, col4 = st.columns(2)
    with col3:
        charm_opts = create_highcharts_bar(
            filtered_snapshot, 
            f"Spot Charm - {selected_ticker}", 
            list(CHARM_METRICS.values()), 
            LABEL_MAP,
            current_price,
            timeseries_data=historical_context_df,
            height=1000
        )
        if charm_opts:
            streamlit_highcharts(charm_opts, key="spot_charm", height=1000)

# ----------------- Render Bottom Row -----------------
with tab2:
    st.subheader("Historical Trajectory")
    
    # Heatmap Section
    gamma_hm_opts = create_highcharts_heatmap(
        filtered_timeseries, 
        f"Gamma Exposure Heatmap - {selected_ticker}", 
        'dealer_gamma_vol',
        current_price,
        is_normalized=st.session_state.normalized_heatmap
    )
    if gamma_hm_opts:
        streamlit_highcharts(gamma_hm_opts, key="hist_gamma_heatmap", height=650)

    # Time-series metrics: Reverting to multi-line "Strike Timeline" chart
    col1, col2 = st.columns(2)
    with col1:
        gamma_flow_opts = create_highcharts_line(
            filtered_timeseries, 
            "Gamma Exposure by Time per Strike", 
            "dealer_gamma_vol"
        )
        if gamma_flow_opts:
            streamlit_highcharts(gamma_flow_opts, key="hist_gamma_line", height=650)
            
    with col2:
        delta_flow_opts = create_highcharts_line(
            filtered_timeseries, 
            "Delta Exposure by Time per Strike", 
            "dealer_delta_vol"
        )
        if delta_flow_opts:
            streamlit_highcharts(delta_flow_opts, key="hist_delta_line", height=650)

    charm_hm_opts = create_highcharts_heatmap(
        filtered_timeseries, 
        f"Charm Exposure Heatmap - {selected_ticker}", 
        'dealer_charm_vol',
        current_price,
        is_normalized=st.session_state.normalized_heatmap
    )
    if charm_hm_opts:
        streamlit_highcharts(charm_hm_opts, key="hist_charm_heatmap", height=650)


# ----------------- Animation: Rerun Trigger -----------------
# We only sleep and rerun here. The actual state increment happens
# at the top of the script on the next run.
if st.session_state.get('is_playing', False):
    # Give AAPL charts more time to breathe (0.3s)
    time.sleep(0.3)
    st.rerun()
