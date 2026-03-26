import streamlit as st
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
with st.sidebar:
    st.header("Data Sync")

    @st.fragment()
    def sync_data_ui():
        col1, col2 = st.columns([3, 1])
        if col1.button("📥 Sync New Data", width='stretch'):
            # Manually trigger a background sync
            coordinator.sync(force_sync=True)
            st.toast("Syncing data...")
        
        if col2.button("🔃", help="Full Refresh"):
            st.cache_data.clear()
            load_full_data(force_sync=True)
            st.rerun()

        # Display last sync time
        _, last_sync = coordinator.get_data()
        if last_sync > 0:
            st.caption(f"Last Synced: {time.strftime('%H:%M:%S', time.localtime(last_sync))}")
        else:
            st.caption("Last Synced: Never")

    sync_data_ui()

    # Auto-refresh option
    st.checkbox("🔄 Auto Refresh (Background)", value=True, key="auto_refresh_enabled")


# --- Background Sync Fragment ---
if 'last_ui_sync' not in st.session_state:
    st.session_state.last_ui_sync = 0.0

@st.fragment(run_every=10)
def background_monitor():
    # If the coordinator has new data since our last UI render, trigger rerun
    _, last_sync = coordinator.get_data()
    
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

# --- Rewind / Replay Mode Control ---
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
    # Sort by timestamp so drop_duplicates keeps the 'latest' ingested (last) if conflicts exist
    ticker_df = ticker_df.sort_values(['timestamp', 'strike'])
    ticker_df = ticker_df.drop_duplicates(subset=['timestamp', 'strike'], keep='last')
        
    return ticker_df

# Cache the processed result for the selected ticker
cache_key = f"processed_df_{selected_ticker}"
# We use the length of the full_df as a simple indicator for cache invalidation
# although @st.cache_data already handles ttl.
last_full_len = len(full_df)

if cache_key not in st.session_state or st.session_state.get('last_full_len', 0) != last_full_len:
    st.session_state[cache_key] = get_processed_ticker_data(selected_ticker, full_df)
    st.session_state.last_full_len = last_full_len

ticker_df = st.session_state[cache_key]
if ticker_df.empty:
    st.warning(f"No data available for ticker: {selected_ticker}")
    st.stop()

# --- Global Max Time ---
latest_timestamp = ticker_df['timestamp'].max()

# --- Rewind / Replay Controls ---
st.sidebar.markdown("---")
# Get unique dates available in the data
available_dates = sorted(list(set(df['timestamp'].dt.date)))

# --- Context and Change Detection ---
current_date_obj = None
if is_rewind and selected_date:
    try:
        current_date_obj = pd.to_datetime(selected_date).date()
    except:
        current_date_obj = None
else:
    current_date_obj = None

if 'last_ticker' not in st.session_state:
    st.session_state.last_ticker = selected_ticker
if 'last_date' not in st.session_state:
    st.session_state.last_date = selected_date

ticker_changed = st.session_state.last_ticker != selected_ticker
date_changed = is_rewind and (st.session_state.last_date != selected_date)

# --- Timestamp Calculation ---
if is_rewind and available_dates:
    # Provide the full historical range for the selected date
    ts_series = ticker_df[ticker_df['date'] == current_date_obj]['timestamp']
    unique_timestamps = sorted([ts for ts in ts_series.unique() if pd.notnull(ts)])
else:
    # Default view: show all available snapshots in the combined buffer
    # This ensures yesterday's history isn't hidden by today's live data.
    unique_timestamps = sorted([ts for ts in ticker_df['timestamp'].unique() if pd.notnull(ts)])

# --- Cache Unique Timestamps for Performance ---
ts_cache_key = f"unique_ts_{selected_ticker}_{selected_date}"
if ts_cache_key not in st.session_state or ticker_changed or date_changed:
    st.session_state[ts_cache_key] = unique_timestamps
else:
    unique_timestamps = st.session_state[ts_cache_key]

# --- State Sync: Apply Changes ---
if ticker_changed or date_changed:
    # Reset state for the new context
    if unique_timestamps:
        st.session_state.rewind_idx = 0 # Start at beginning for new date/ticker
        st.session_state.rewind_slider = unique_timestamps[0]
    else:
        st.session_state.rewind_idx = -1
        
    st.session_state.last_ticker = selected_ticker
    st.session_state.last_date = selected_date
    st.session_state.is_playing = False # Stop playback on change
    st.toast(f"Switched context to {selected_ticker}" if ticker_changed else f"Date changed to {selected_date}")

# --- Animation: State Update (MUST BE BEFORE WIDGET) ---
# We must ensure we're using timestamps for the selected date
if st.session_state.get('is_playing', False) and len(unique_timestamps) > 0:
    if st.session_state.rewind_idx < len(unique_timestamps) - 1:
        st.session_state.rewind_idx += 1
        st.session_state.rewind_slider = unique_timestamps[st.session_state.rewind_idx]
    else:
        st.session_state.is_playing = False
        st.toast("Reached end of historical data")

if is_rewind and len(unique_timestamps) > 1:
    # Handle animation logic
    # Replay controls: Back, Play/Stop, Forward
    ctrl_cols = st.sidebar.columns([1, 1.2, 1])
    
    # Back button
    if ctrl_cols[0].button("⏪", width='stretch'):
        st.session_state.is_playing = False
        if st.session_state.rewind_idx > 0:
            st.session_state.rewind_idx -= 1
            st.session_state.rewind_slider = unique_timestamps[st.session_state.rewind_idx]
            st.rerun()

    # Dynamic Play/Stop button
    if st.session_state.get('is_playing', False):
        if ctrl_cols[1].button("⏹️ Stop", width='stretch'):
            st.session_state.is_playing = False
            st.rerun()
    else:
        if ctrl_cols[1].button("▶️ Play", width='stretch'):
            # If at end, loop back
            if st.session_state.rewind_idx >= len(unique_timestamps) - 1:
                st.session_state.rewind_idx = 0
            st.session_state.is_playing = True
            st.rerun()

    # Forward button
    if ctrl_cols[2].button("⏩", width='stretch'):
        st.session_state.is_playing = False
        if st.session_state.rewind_idx < len(unique_timestamps) - 1:
            st.session_state.rewind_idx += 1
            st.session_state.rewind_slider = unique_timestamps[st.session_state.rewind_idx]
            st.rerun()

    # If index is -1 or out of bounds, set to last
    if st.session_state.rewind_idx == -1 or st.session_state.rewind_idx >= len(unique_timestamps):
        st.session_state.rewind_idx = len(unique_timestamps) - 1
        st.session_state.rewind_slider = unique_timestamps[st.session_state.rewind_idx]

    # Slider is linked to the session state key "rewind_slider"
    selected_time = st.sidebar.select_slider(
        "Select Snapshot Time",
        options=unique_timestamps,
        format_func=lambda x: x.strftime("%H:%M:%S"),
        key="rewind_slider"
    )
    
    # Sync rewind_idx with whatever the slider is currently at
    try:
        # Optimization: use a local variable to avoid continuous session state lookups
        cur_slider_val = st.session_state.rewind_slider
        st.session_state.rewind_idx = unique_timestamps.index(cur_slider_val)
    except ValueError:
        # If the value is missing from the list (happens if data refreshes), find closest
        st.session_state.rewind_idx = len(unique_timestamps) - 1
        st.session_state.rewind_slider = unique_timestamps[st.session_state.rewind_idx]
        
    view_timestamp = st.session_state.rewind_slider
    st.sidebar.info(f"Viewing data from: {view_timestamp.strftime('%H:%M:%S')}")
else:
    view_timestamp = latest_timestamp
    st.session_state.is_playing = False
    if is_rewind and len(unique_timestamps) <= 1:
        st.sidebar.warning(f"Not enough historical data for {selected_date} to rewind.")

# --- Dynamic Timeframe Filtering ---
# Filter data to include all snapshots up to the current selection
# To match index.html, we don't restrict to a single calendar day
ticker_df = ticker_df[ticker_df['timestamp'] <= view_timestamp]

timeframe = st.sidebar.selectbox("Timeframe", ["Last 15 Minutes", "Last 30 Minutes", "Last 1 Hour", "Last 4 Hours", "Full Day"], index=4)

if timeframe != "Full Day":
    minutes_map = {"Last 15 Minutes": 15, "Last 30 Minutes": 30, "Last 1 Hour": 60, "Last 4 Hours": 240}
    # Limit the view window based on selection
    cutoff_time = view_timestamp - pd.Timedelta(minutes=minutes_map[timeframe])
    ticker_df = ticker_df[ticker_df['timestamp'] >= cutoff_time]

# Define snapshot for the bar charts
snapshot_df = ticker_df[ticker_df['timestamp'] == view_timestamp]
if snapshot_df.empty:
    # If the timeframe filter cut out the view_timestamp (unlikely but possible), 
    # we use the full ticker data for the snapshot
    snapshot_df = df[df['timestamp'] == view_timestamp]

if snapshot_df.empty:
    st.warning(f"No snapshot data available for {selected_ticker} at {view_timestamp}")
    st.stop()

# Determine current price for strike centering
if not snapshot_df.empty and pd.notnull(snapshot_df['price'].iloc[0]):
    current_price = float(snapshot_df['price'].iloc[0])
elif not ticker_df.empty:
    # Use latest available price for this ticker
    valid_prices = ticker_df['price'].dropna()
    current_price = float(valid_prices.iloc[-1]) if not valid_prices.empty else 100.0
else:
    current_price = 100.0

# --- Strike and Metric Selection Toggles ---
st.sidebar.markdown("---")
total_strikes = st.sidebar.slider("Total Number of Strikes", min_value=1, max_value=50, value=10)

GAMMA_METRICS = {
    'Open Interest': 'dealer_gamma_oi',
    'Volume': 'dealer_gamma_vol',
    'Call Vol': 'call_gamma_vol',
    'Put Vol': 'put_gamma_vol'
}
DELTA_METRICS = {
    'Open Interest / 100': 'dealer_delta_oi',
    'Volume': 'dealer_delta_vol',
    'Call Vol': 'call_delta_vol',
    'Put Vol': 'put_delta_vol'
}
CHARM_METRICS = {
    'Open Interest': 'dealer_charm_oi',
    'Volume': 'dealer_charm_vol',
    'Call Vol': 'call_charm_vol',
    'Put Vol': 'put_charm_vol'
}

LABEL_MAP = {
    **{v: k for k, v in GAMMA_METRICS.items()}, 
    **{v: k for k, v in DELTA_METRICS.items()},
    **{v: k for k, v in CHARM_METRICS.items()}
}



st.session_state.normalized_heatmap = st.sidebar.checkbox("Normalized Heatmap", value=st.session_state.normalized_heatmap)


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

def create_horizontal_bar_chart(data, title, value_vars, label_map, current_price, timeseries_data=None):
    if data.empty:
        return go.Figure()

    # Clean data
    data = data.dropna(subset=['strike'])
    # Numeric sorting to ensure highest prices are at the TOP (index N)
    data = data.sort_values('strike', ascending=True)
    data['strike_label'] = data['strike'].map(lambda x: f"${x:,.2f}")
    
    fig = go.Figure()
    
    colors = px.colors.qualitative.Plotly
    
    # Add Bars for each metric
    for i, var in enumerate(value_vars):
        label = label_map[var]
        
        fig.add_trace(go.Bar(
            y=data['strike_label'],
            x=data[var],
            name=label,
            orientation='h',
            marker_color=colors[i % len(colors)],
            hovertemplate="Strike: %{y}<br>Metric: " + label + "<br>Value: %{x:$,.2f}<extra></extra>"
        ))

    # Add historical scatter points (1m, 5m etc ago)
    if timeseries_data is not None and not timeseries_data.empty:
        target_mins = [1, 5, 10, 15, 30]
        ts_valid = timeseries_data['timestamp'][timeseries_data['timestamp'].notnull()]
        
        if not ts_valid.empty:
            latest_ts = ts_valid.max()
            available_ts = sorted([pd.Timestamp(ts) for ts in ts_valid.unique()])
            
            for mins in target_mins:
                target_ts = latest_ts - pd.Timedelta(minutes=mins)
                closest_ts = min(available_ts, key=lambda x: abs((pd.Timestamp(x) - target_ts).total_seconds()))
                
                # Buffer to avoid showing current data
                if abs((pd.Timestamp(closest_ts) - latest_ts).total_seconds()) < 20:
                    continue
                    
                snapshot_hist = timeseries_data[timeseries_data['timestamp'] == closest_ts]
                
                for i, var in enumerate(value_vars):
                    label = label_map[var]
                    hist_val = snapshot_hist[snapshot_hist['strike'].isin(data['strike'])].copy()
                    if not hist_val.empty:
                        hist_val['strike_label'] = hist_val['strike'].map(lambda x: f"${x:,.2f}")
                        
                        fig.add_trace(go.Scatter(
                            y=hist_val['strike_label'],
                            x=hist_val[var],
                            name=f"{label} ({mins}m ago)",
                            mode='markers',
                            marker=dict(symbol='circle-open', size=10, color=colors[i % len(colors)], line=dict(width=1, color='white')),
                            legendgroup=label,
                            showlegend=False,
                            hovertemplate=f"Strike: %{{y}}<br>{label} ({mins}m ago): %{{x:$,.2f}}<extra></extra>"
                        ))

    # Add price line
    # Since Y is categorical, we use paper coordinates for X to span the whole chart
    fig.add_shape(
        type="line",
        xref="paper", yref="y",
        x0=0, x1=1,
        y0=f"${current_price:,.2f}", y1=f"${current_price:,.2f}",
        line=dict(color="violet", width=2, dash="dash"),
    )
    
    fig.add_annotation(
        xref="paper", yref="y",
        x=1, y=f"${current_price:,.2f}",
        text=f"Price: ${current_price:,.2f}",
        showarrow=False,
        font=dict(color="violet", size=12),
        bgcolor="black",
        opacity=0.8,
        yshift=10
    )

    fig.update_layout(
        title=title,
        template="plotly_dark",
        barmode='group',
        height=1000,
        margin=dict(l=0, r=0, t=50, b=0),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis=dict(title="Dealer Exposure", gridcolor="#31333f"),
        yaxis=dict(title="Strike", gridcolor="#31333f", categoryorder='array', categoryarray=data['strike_label'])
    )
    
    return fig

def create_line_chart(data, title, y_var):    
    if data.empty:
        return go.Figure()

    fig = go.Figure()
    unique_strikes = sorted(data['strike'].unique().tolist(), reverse=True)
    
    for strike in unique_strikes:
        strike_data = data[data['strike'] == strike]
        fig.add_trace(go.Scatter(
            x=strike_data['timestamp'],
            y=strike_data[y_var],
            name=f"${strike:,.2f}",
            mode='lines',
            hovertemplate="Time: %{x}<br>Value: %{y:$,.2f}<extra></extra>"
        ))

    fig.update_layout(
        title=title,
        template="plotly_dark",
        height=600,
        margin=dict(l=0, r=0, t=50, b=0),
        xaxis=dict(title=None, gridcolor="#31333f"),
        yaxis=dict(title="Exposure", gridcolor="#31333f"),
        legend=dict(title="Strikes")
    )
    
    return fig

def create_heatmap(data, title, metric, strikes, normalized=True):
    if data.empty or not strikes:
        return go.Figure()
    
    # Filter strikes
    data = data[data['strike'].isin(strikes)].copy()
    
    # Downsample to 1m for performance
    data['minute'] = data['timestamp'].dt.floor('1min')
    heatmap_df = data.groupby(['minute', 'strike'])[metric].mean().reset_index()
    
    # Prepare matrix
    hm_pivot = heatmap_df.pivot(index='strike', columns='minute', values=metric).sort_index(ascending=True)
    
    z_values = hm_pivot.values
    if normalized:
        max_per_col = hm_pivot.abs().max(axis=0)
        z_values = hm_pivot.div(max_per_col.replace(0, 1), axis=1).values
        color_range = [-1, 1]
    else:
        abs_max = heatmap_df[metric].abs().max() or 1.0
        color_range = [-abs_max, abs_max]

    fig = go.Figure(data=go.Heatmap(
        z=z_values,
        x=hm_pivot.columns,
        y=hm_pivot.index, # Pass numeric index for linear scale
        colorscale=[[0, 'red'], [0.5, 'black'], [1, 'green']],
        zmin=color_range[0],
        zmax=color_range[1],
        hoverongaps=False,
        hovertemplate="Time: %{x}<br>Strike: %{y}<br>Value: %{customdata:$,.2f}<extra></extra>",
        customdata=hm_pivot.values
    ))

    # Re-add Price Line overlay
    price_data = data.groupby('minute')['price'].last().reset_index()
    fig.add_trace(go.Scatter(
        x=price_data['minute'],
        y=price_data['price'],
        mode='lines',
        name='Price',
        line=dict(color='white', width=2, dash='dot'),
        hoverinfo='skip'
    ))

    fig.update_layout(
        title=title,
        template="plotly_dark",
        height=500,
        margin=dict(l=0, r=0, t=50, b=0),
        xaxis=dict(title=None),
        yaxis=dict(title="Strike", type='linear') # Linear for proportional price tracking
    )
    
    return fig

# --- Live Stats Bar ---
st.markdown("---")
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

    col1, col2, col3 = st.columns(3)
    with col1:
        gamma_chart = create_horizontal_bar_chart(
            filtered_snapshot, 
            f"Spot Gamma - {selected_ticker} ({view_timestamp.strftime('%H:%M:%S')})", 
            list(GAMMA_METRICS.values()), 
            LABEL_MAP,
            current_price,
            timeseries_data=historical_context_df
        )
        st.plotly_chart(gamma_chart, width='stretch')

    with col2:
        delta_chart = create_horizontal_bar_chart(
            filtered_snapshot, 
            f"Spot Delta - {selected_ticker} ({view_timestamp.strftime('%H:%M:%S')})", 
            list(DELTA_METRICS.values()), 
            LABEL_MAP,
            current_price,
            timeseries_data=historical_context_df
        )
        st.plotly_chart(delta_chart, width='stretch')

    with col3:
        charm_chart = create_horizontal_bar_chart(
            filtered_snapshot, 
            f"Spot Charm - {selected_ticker} ({view_timestamp.strftime('%H:%M:%S')})", 
            list(CHARM_METRICS.values()), 
            LABEL_MAP,
            current_price,
            timeseries_data=historical_context_df
        )
        st.plotly_chart(charm_chart, width='stretch')

# ----------------- Render Bottom Row -----------------
with tab2:
    st.subheader("Historical Trajectory")
    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(create_line_chart(
            filtered_timeseries, 
            f"Dealer Gamma Exposure by Time per Strike - {selected_ticker}", 
            'dealer_gamma_vol'
        ), width='stretch')

    with col2:
        st.plotly_chart(create_line_chart(
            filtered_timeseries, 
            f"Dealer Delta Exposure by Time per Strike - {selected_ticker}", 
            'dealer_delta_vol'
        ), width='stretch')

    st.markdown("---")
    st.subheader("Exposure Heatmaps")
    st.caption("Lowering SVG node count via 1-minute downsampling for high-density rendering.")
    
    col3, col4 = st.columns(2)
    with col3:
        hm_gamma = create_heatmap(
            ticker_df, 
            f"Gamma Concentration Heatmap - {selected_ticker}", 
            'dealer_gamma_vol', 
            valid_strikes,
            normalized=st.session_state.normalized_heatmap
        )
        st.plotly_chart(hm_gamma, width='stretch')

    with col4:
        hm_charm = create_heatmap(
            ticker_df, 
            f"Charm Concentration Heatmap - {selected_ticker}", 
            'dealer_charm_vol', 
            valid_strikes,
            normalized=st.session_state.normalized_heatmap
        )
        st.plotly_chart(hm_charm, width='stretch')


# ----------------- Animation: Rerun Trigger -----------------
# We only sleep and rerun here. The actual state increment happens
# at the top of the script on the next run.
if st.session_state.get('is_playing', False):
    # Give AAPL charts more time to breathe (0.3s)
    time.sleep(0.3)
    st.rerun()