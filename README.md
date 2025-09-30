# SMC Trading System

Automated trading system based on Smart Money Concepts (Order Blocks, BOS, CHoCH) with dual execution modes.

## Project Structure

```
smc_trading_system/
├── config/              # Configuration files
├── core/                # Core business logic
│   ├── data/           # Data management
│   ├── strategy/       # Trading strategy (OB detection)
│   ├── execution/      # Trade execution
│   ├── risk/           # Risk management
│   └── utils/          # Utilities
├── brokers/            # Broker integrations
├── data/               # Runtime data (gitignored)
├── logs/               # Log files (gitignored)
├── tests/              # Unit tests
└── scripts/            # Utility scripts
```

## Setup

1. **Create virtual environment:**
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Mac/Linux
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment:**
   ```bash
   cp .env.example .env
   # Edit .env with your API keys and Telegram credentials
   ```

4. **Fetch historical data:**
   ```bash
   python scripts/fetch_historical_data.py
   ```

5. **Run system:**
   ```bash
   python main.py
   ```

## Trading Modes

### Mode A: Candle Close Entry
- Wait for 15m candle to close
- Check if OB was touched and candle closed favorably
- Enter with market order

### Mode B: Limit Order Entry
- Place limit order at OB entry price (20% penetration)
- Order fills mid-candle when price reaches level
- Cancel order if OB invalidates

## Configuration

Edit `config/sub_account_1.json` and `config/sub_account_2.json` to adjust:
- Position sizing (40% for fresh OBs, 30% for breakers)
- Leverage (20x for fresh OBs, 10x for breakers)
- Target/trailing stop parameters
- Risk management rules

## Development

- Follow the **Master Project Tracker** for current progress
- Run tests: `pytest tests/`
- Format code: `black .`

## Safety

- Always test with paper trading first
- Start with minimum capital ($20 per symbol)
- Monitor Telegram notifications
- Check logs regularly

## Support

Refer to:
1. Master Project Tracker (project state)
2. Architecture Blueprint (system design)
3. Git commit history
4. Code comments
