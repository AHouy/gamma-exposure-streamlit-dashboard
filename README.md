# Gamma Exposure Streamlit Dashboard

This folder contains a quick python-based reporting dashboard for analyzing exported `data.csv` historical snapshots. It parses out Gamma and Delta distributions by Strike using Altair visualizations.

### Prerequisites

You need `python3` to run the application natively.

1. Turn on the python virtual-environment:

   ```bash
   source venv/bin/activate
   ```

2. Make sure dependencies are built:
   ```bash
   pip install -r requirements.txt
   ```

### Running the Dashboard

To start the interface, launch Streamlit mapped to your `venv` instance.

_Note: Since Python `3.14` has some issues with Google `protobuf` C interfaces, we run Streamlit natively with `PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python`:_

```bash
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python streamlit run app.py
```

Then visit `http://localhost:8501` to view your charts filterable by Ticker, Expiration, and Time Series!
