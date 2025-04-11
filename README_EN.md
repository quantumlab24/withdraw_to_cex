# 🔁 Web3 BRIDGE/WITHDRAW to CEX (evm-address)

A Python script for automating the management of multiple Web3 wallets: balance checks, bridging, withdrawals to CEX, logging, and multithreading.

## 📦 Features

- ✅ Supports networks: Ethereum, Arbitrum, Optimism, Base, Scroll, zkSync
- 🔁 **Auto-bridging of funds from Scroll** (if `BRIDGE_TO_RANDOM_CHAIN = True`):
  - To the network with the **highest balance**, or
  - To a **random available chain**
- 💸 **Auto-withdraw of ETH to CEX** in the following networks listed in the code array `CHAINS_FOR_WITHDRAW`:

```python
CHAINS_FOR_WITHDRAW = {
    1,     # Ethereum
    10,    # Optimism
    42161, # Arbitrum
    8453   # Base
}
```

- 💰 Real-time ETH price via Binance and Bybit API
- 📊 Logs all activity to the Excel file `wallet.xlsx` (column `STATUS`)
- ⚙️ Multithreading with `ThreadPoolExecutor`
- 🔐 Supports seed phrases (BIP-44 → private key)
- 🧠 Smart logic: skips actions for wallets with insufficient balance

---

## 📁 Project Structure

- `main.py` — main script
- `utils.py` — logging functions
- `excel_functions.py` — Excel file handling
- `wallet.xlsx` — spreadsheet containing wallets

---

## ⚙️ Setup

1. Make sure Python 3.11+ is installed
2. Install dependencies:

```bash
python -m venv .venv
```
```bash
.venv\Scripts\activate
```
```bash
pip install -r requirements.txt
```

Contents of `requirements.txt`:
```text
requests~=2.32.3
web3~=7.8.0
loguru~=0.7.3
eth-account~=0.13.5
mnemonic~=0.21
openpyxl~=3.1.5
eth-keys~=0.6.1
bip32utils~=0.3.post4
```

---

## 📋 Excel File Format: `wallet.xlsx`

| NUMBER_WALLET | SEED            | ADDRESS_TO         | STATUS        |
|---------------|------------------|---------------------|---------------|
| 1             | seed phrase      | ETH withdrawal addr | (empty/log)   |

---

## 🚀 Launch

```bash
python main.py
```

The script will:
1. Check wallet balances
2. Bridge funds from Scroll (if needed) using Stargate Bridge (v2) to the optimal/random chain
3. Send ETH to CEX in supported chains
4. Log actions to Excel

---

## 📜 Example Log (`STATUS` column output)

```
10/04/2025 23:26
BRIDGE:534352 => 10:SUCCESS
SEND:1 => 0x3d ***:SUCCESS
SEND:10 => 0x3d ***:SUCCESS
```

---

## ⚠️ Warning

- This script handles **real assets**.
- Always test on burner wallets before production use.

## 📄 License

- Provided "as is" for educational and research purposes.

---