import concurrent
import json
import random
import time
import traceback
from datetime import datetime
from decimal import Decimal
from typing import Optional, Any, Dict
from enum import Enum
import concurrent.futures
import threading
from mnemonic import Mnemonic
from bip32utils import BIP32Key, BIP32_HARDEN
from eth_keys import keys
import requests
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from eth_account import Account
from utils import logger

from excel_functions import get_profile_for_work, write_cell

# Количество потоков
MAX_THREADS = 5
# Вывод средств из сетей CHAINS_FOR_WITHDRAW максимальной суммы
WITHDRAW_TO_CEX = True
# Обмен ETH в сетях CHAINS_FOR_BRIDGE в сеть с наибольшим балансом (CHAINS_FOR_WITHDRAW) для последующего вывода на биржу или в рандомную сеть (если балансы нулевые)
BRIDGE_TO_RANDOM_CHAIN = True
# Минимальный остаток на кошельке для вывода или бриджа
MIN_USD_AMOUNT = 0.1

abi_erc20 = """[{"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"approve","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"account","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"transfer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"spender","type":"address"}],"name":"allowance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"}]"""

stargate_native_pool_abi = """[{"inputs":[{"internalType":"string","name":"_lpTokenName","type":"string"},{"internalType":"string","name":"_lpTokenSymbol","type":"string"},{"internalType":"uint8","name":"_tokenDecimals","type":"uint8"},{"internalType":"uint8","name":"_sharedDecimals","type":"uint8"},{"internalType":"address","name":"_endpoint","type":"address"},{"internalType":"address","name":"_owner","type":"address"}],"stateMutability":"nonpayable","type":"constructor"},{"inputs":[],"name":"InvalidLocalDecimals","type":"error"},{"inputs":[],"name":"Path_AlreadyHasCredit","type":"error"},{"inputs":[],"name":"Path_InsufficientCredit","type":"error"},{"inputs":[],"name":"Path_UnlimitedCredit","type":"error"},{"inputs":[{"internalType":"uint256","name":"amountLD","type":"uint256"},{"internalType":"uint256","name":"minAmountLD","type":"uint256"}],"name":"SlippageExceeded","type":"error"},{"inputs":[],"name":"Stargate_InsufficientFare","type":"error"},{"inputs":[],"name":"Stargate_InvalidAmount","type":"error"},{"inputs":[],"name":"Stargate_InvalidPath","type":"error"},{"inputs":[],"name":"Stargate_InvalidTokenDecimals","type":"error"},{"inputs":[],"name":"Stargate_LzTokenUnavailable","type":"error"},{"inputs":[],"name":"Stargate_OnlyTaxi","type":"error"},{"inputs":[],"name":"Stargate_OutflowFailed","type":"error"},{"inputs":[],"name":"Stargate_Paused","type":"error"},{"inputs":[],"name":"Stargate_RecoverTokenUnsupported","type":"error"},{"inputs":[],"name":"Stargate_ReentrantCall","type":"error"},{"inputs":[],"name":"Stargate_SlippageTooHigh","type":"error"},{"inputs":[],"name":"Stargate_Unauthorized","type":"error"},{"inputs":[],"name":"Stargate_UnreceivedTokenNotFound","type":"error"},{"inputs":[],"name":"Transfer_ApproveFailed","type":"error"},{"inputs":[],"name":"Transfer_TransferFailed","type":"error"},{"anonymous":false,"inputs":[{"components":[{"internalType":"address","name":"feeLib","type":"address"},{"internalType":"address","name":"planner","type":"address"},{"internalType":"address","name":"treasurer","type":"address"},{"internalType":"address","name":"tokenMessaging","type":"address"},{"internalType":"address","name":"creditMessaging","type":"address"},{"internalType":"address","name":"lzToken","type":"address"}],"indexed":false,"internalType":"struct StargateBase.AddressConfig","name":"config","type":"tuple"}],"name":"AddressConfigSet","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint32","name":"srcEid","type":"uint32"},{"components":[{"internalType":"uint32","name":"srcEid","type":"uint32"},{"internalType":"uint64","name":"amount","type":"uint64"}],"indexed":false,"internalType":"struct Credit[]","name":"credits","type":"tuple[]"}],"name":"CreditsReceived","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint32","name":"dstEid","type":"uint32"},{"components":[{"internalType":"uint32","name":"srcEid","type":"uint32"},{"internalType":"uint64","name":"amount","type":"uint64"}],"indexed":false,"internalType":"struct Credit[]","name":"credits","type":"tuple[]"}],"name":"CreditsSent","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"payer","type":"address"},{"indexed":true,"internalType":"address","name":"receiver","type":"address"},{"indexed":false,"internalType":"uint256","name":"amountLD","type":"uint256"}],"name":"Deposited","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint32","name":"dstEid","type":"uint32"},{"indexed":false,"internalType":"bool","name":"oft","type":"bool"}],"name":"OFTPathSet","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"bytes32","name":"guid","type":"bytes32"},{"indexed":false,"internalType":"uint32","name":"srcEid","type":"uint32"},{"indexed":true,"internalType":"address","name":"toAddress","type":"address"},{"indexed":false,"internalType":"uint256","name":"amountReceivedLD","type":"uint256"}],"name":"OFTReceived","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"bytes32","name":"guid","type":"bytes32"},{"indexed":false,"internalType":"uint32","name":"dstEid","type":"uint32"},{"indexed":true,"internalType":"address","name":"fromAddress","type":"address"},{"indexed":false,"internalType":"uint256","name":"amountSentLD","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amountReceivedLD","type":"uint256"}],"name":"OFTSent","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"bool","name":"paused","type":"bool"}],"name":"PauseSet","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"}],"name":"PlannerFeeWithdrawn","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"payer","type":"address"},{"indexed":true,"internalType":"address","name":"receiver","type":"address"},{"indexed":false,"internalType":"uint256","name":"amountLD","type":"uint256"}],"name":"Redeemed","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint64","name":"amountSD","type":"uint64"}],"name":"TreasuryFeeAdded","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint64","name":"amountSD","type":"uint64"}],"name":"TreasuryFeeWithdrawn","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"bytes32","name":"guid","type":"bytes32"},{"indexed":false,"internalType":"uint8","name":"index","type":"uint8"},{"indexed":false,"internalType":"uint32","name":"srcEid","type":"uint32"},{"indexed":false,"internalType":"address","name":"receiver","type":"address"},{"indexed":false,"internalType":"uint256","name":"amountLD","type":"uint256"},{"indexed":false,"internalType":"bytes","name":"composeMsg","type":"bytes"}],"name":"UnreceivedTokenCached","type":"event"},{"stateMutability":"payable","type":"fallback"},{"inputs":[{"internalType":"uint256","name":"_amountLD","type":"uint256"}],"name":"addTreasuryFee","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[],"name":"approvalRequired","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"pure","type":"function"},{"inputs":[],"name":"deficitOffset","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_receiver","type":"address"},{"internalType":"uint256","name":"_amountLD","type":"uint256"}],"name":"deposit","outputs":[{"internalType":"uint256","name":"amountLD","type":"uint256"}],"stateMutability":"payable","type":"function"},{"inputs":[],"name":"endpoint","outputs":[{"internalType":"contract ILayerZeroEndpointV2","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getAddressConfig","outputs":[{"components":[{"internalType":"address","name":"feeLib","type":"address"},{"internalType":"address","name":"planner","type":"address"},{"internalType":"address","name":"treasurer","type":"address"},{"internalType":"address","name":"tokenMessaging","type":"address"},{"internalType":"address","name":"creditMessaging","type":"address"},{"internalType":"address","name":"lzToken","type":"address"}],"internalType":"struct StargateBase.AddressConfig","name":"","type":"tuple"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getTransferGasLimit","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"localEid","outputs":[{"internalType":"uint32","name":"","type":"uint32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"lpToken","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"oftVersion","outputs":[{"internalType":"bytes4","name":"interfaceId","type":"bytes4"},{"internalType":"uint64","name":"version","type":"uint64"}],"stateMutability":"pure","type":"function"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint32","name":"eid","type":"uint32"}],"name":"paths","outputs":[{"internalType":"uint64","name":"credit","type":"uint64"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"plannerFee","outputs":[{"internalType":"uint256","name":"available","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"poolBalance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"components":[{"internalType":"uint32","name":"dstEid","type":"uint32"},{"internalType":"bytes32","name":"to","type":"bytes32"},{"internalType":"uint256","name":"amountLD","type":"uint256"},{"internalType":"uint256","name":"minAmountLD","type":"uint256"},{"internalType":"bytes","name":"extraOptions","type":"bytes"},{"internalType":"bytes","name":"composeMsg","type":"bytes"},{"internalType":"bytes","name":"oftCmd","type":"bytes"}],"internalType":"struct SendParam","name":"_sendParam","type":"tuple"}],"name":"quoteOFT","outputs":[{"components":[{"internalType":"uint256","name":"minAmountLD","type":"uint256"},{"internalType":"uint256","name":"maxAmountLD","type":"uint256"}],"internalType":"struct OFTLimit","name":"limit","type":"tuple"},{"components":[{"internalType":"int256","name":"feeAmountLD","type":"int256"},{"internalType":"string","name":"description","type":"string"}],"internalType":"struct OFTFeeDetail[]","name":"oftFeeDetails","type":"tuple[]"},{"components":[{"internalType":"uint256","name":"amountSentLD","type":"uint256"},{"internalType":"uint256","name":"amountReceivedLD","type":"uint256"}],"internalType":"struct OFTReceipt","name":"receipt","type":"tuple"}],"stateMutability":"view","type":"function"},{"inputs":[{"components":[{"internalType":"uint32","name":"dstEid","type":"uint32"},{"internalType":"bytes32","name":"to","type":"bytes32"},{"internalType":"uint256","name":"amountLD","type":"uint256"},{"internalType":"uint256","name":"minAmountLD","type":"uint256"},{"internalType":"bytes","name":"extraOptions","type":"bytes"},{"internalType":"bytes","name":"composeMsg","type":"bytes"},{"internalType":"bytes","name":"oftCmd","type":"bytes"}],"internalType":"struct SendParam","name":"_sendParam","type":"tuple"},{"internalType":"bool","name":"_payInLzToken","type":"bool"}],"name":"quoteRedeemSend","outputs":[{"components":[{"internalType":"uint256","name":"nativeFee","type":"uint256"},{"internalType":"uint256","name":"lzTokenFee","type":"uint256"}],"internalType":"struct MessagingFee","name":"fee","type":"tuple"}],"stateMutability":"view","type":"function"},{"inputs":[{"components":[{"internalType":"uint32","name":"dstEid","type":"uint32"},{"internalType":"bytes32","name":"to","type":"bytes32"},{"internalType":"uint256","name":"amountLD","type":"uint256"},{"internalType":"uint256","name":"minAmountLD","type":"uint256"},{"internalType":"bytes","name":"extraOptions","type":"bytes"},{"internalType":"bytes","name":"composeMsg","type":"bytes"},{"internalType":"bytes","name":"oftCmd","type":"bytes"}],"internalType":"struct SendParam","name":"_sendParam","type":"tuple"},{"internalType":"bool","name":"_payInLzToken","type":"bool"}],"name":"quoteSend","outputs":[{"components":[{"internalType":"uint256","name":"nativeFee","type":"uint256"},{"internalType":"uint256","name":"lzTokenFee","type":"uint256"}],"internalType":"struct MessagingFee","name":"fee","type":"tuple"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint32","name":"_srcEid","type":"uint32"},{"components":[{"internalType":"uint32","name":"srcEid","type":"uint32"},{"internalType":"uint64","name":"amount","type":"uint64"}],"internalType":"struct Credit[]","name":"_credits","type":"tuple[]"}],"name":"receiveCredits","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"components":[{"internalType":"uint32","name":"srcEid","type":"uint32"},{"internalType":"bytes32","name":"sender","type":"bytes32"},{"internalType":"uint64","name":"nonce","type":"uint64"}],"internalType":"struct Origin","name":"_origin","type":"tuple"},{"internalType":"bytes32","name":"_guid","type":"bytes32"},{"internalType":"uint8","name":"_seatNumber","type":"uint8"},{"internalType":"address","name":"_receiver","type":"address"},{"internalType":"uint64","name":"_amountSD","type":"uint64"}],"name":"receiveTokenBus","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"components":[{"internalType":"uint32","name":"srcEid","type":"uint32"},{"internalType":"bytes32","name":"sender","type":"bytes32"},{"internalType":"uint64","name":"nonce","type":"uint64"}],"internalType":"struct Origin","name":"_origin","type":"tuple"},{"internalType":"bytes32","name":"_guid","type":"bytes32"},{"internalType":"address","name":"_receiver","type":"address"},{"internalType":"uint64","name":"_amountSD","type":"uint64"},{"internalType":"bytes","name":"_composeMsg","type":"bytes"}],"name":"receiveTokenTaxi","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_token","type":"address"},{"internalType":"address","name":"_to","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"}],"name":"recoverToken","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"_amountLD","type":"uint256"},{"internalType":"address","name":"_receiver","type":"address"}],"name":"redeem","outputs":[{"internalType":"uint256","name":"amountLD","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"components":[{"internalType":"uint32","name":"dstEid","type":"uint32"},{"internalType":"bytes32","name":"to","type":"bytes32"},{"internalType":"uint256","name":"amountLD","type":"uint256"},{"internalType":"uint256","name":"minAmountLD","type":"uint256"},{"internalType":"bytes","name":"extraOptions","type":"bytes"},{"internalType":"bytes","name":"composeMsg","type":"bytes"},{"internalType":"bytes","name":"oftCmd","type":"bytes"}],"internalType":"struct SendParam","name":"_sendParam","type":"tuple"},{"components":[{"internalType":"uint256","name":"nativeFee","type":"uint256"},{"internalType":"uint256","name":"lzTokenFee","type":"uint256"}],"internalType":"struct MessagingFee","name":"_fee","type":"tuple"},{"internalType":"address","name":"_refundAddress","type":"address"}],"name":"redeemSend","outputs":[{"components":[{"internalType":"bytes32","name":"guid","type":"bytes32"},{"internalType":"uint64","name":"nonce","type":"uint64"},{"components":[{"internalType":"uint256","name":"nativeFee","type":"uint256"},{"internalType":"uint256","name":"lzTokenFee","type":"uint256"}],"internalType":"struct MessagingFee","name":"fee","type":"tuple"}],"internalType":"struct MessagingReceipt","name":"msgReceipt","type":"tuple"},{"components":[{"internalType":"uint256","name":"amountSentLD","type":"uint256"},{"internalType":"uint256","name":"amountReceivedLD","type":"uint256"}],"internalType":"struct OFTReceipt","name":"oftReceipt","type":"tuple"}],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"address","name":"_owner","type":"address"}],"name":"redeemable","outputs":[{"internalType":"uint256","name":"amountLD","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"renounceOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bytes32","name":"_guid","type":"bytes32"},{"internalType":"uint8","name":"_index","type":"uint8"},{"internalType":"uint32","name":"_srcEid","type":"uint32"},{"internalType":"address","name":"_receiver","type":"address"},{"internalType":"uint256","name":"_amountLD","type":"uint256"},{"internalType":"bytes","name":"_composeMsg","type":"bytes"}],"name":"retryReceiveToken","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"components":[{"internalType":"uint32","name":"dstEid","type":"uint32"},{"internalType":"bytes32","name":"to","type":"bytes32"},{"internalType":"uint256","name":"amountLD","type":"uint256"},{"internalType":"uint256","name":"minAmountLD","type":"uint256"},{"internalType":"bytes","name":"extraOptions","type":"bytes"},{"internalType":"bytes","name":"composeMsg","type":"bytes"},{"internalType":"bytes","name":"oftCmd","type":"bytes"}],"internalType":"struct SendParam","name":"_sendParam","type":"tuple"},{"components":[{"internalType":"uint256","name":"nativeFee","type":"uint256"},{"internalType":"uint256","name":"lzTokenFee","type":"uint256"}],"internalType":"struct MessagingFee","name":"_fee","type":"tuple"},{"internalType":"address","name":"_refundAddress","type":"address"}],"name":"send","outputs":[{"components":[{"internalType":"bytes32","name":"guid","type":"bytes32"},{"internalType":"uint64","name":"nonce","type":"uint64"},{"components":[{"internalType":"uint256","name":"nativeFee","type":"uint256"},{"internalType":"uint256","name":"lzTokenFee","type":"uint256"}],"internalType":"struct MessagingFee","name":"fee","type":"tuple"}],"internalType":"struct MessagingReceipt","name":"msgReceipt","type":"tuple"},{"components":[{"internalType":"uint256","name":"amountSentLD","type":"uint256"},{"internalType":"uint256","name":"amountReceivedLD","type":"uint256"}],"internalType":"struct OFTReceipt","name":"oftReceipt","type":"tuple"}],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"uint32","name":"_dstEid","type":"uint32"},{"components":[{"internalType":"uint32","name":"srcEid","type":"uint32"},{"internalType":"uint64","name":"amount","type":"uint64"},{"internalType":"uint64","name":"minAmount","type":"uint64"}],"internalType":"struct TargetCredit[]","name":"_credits","type":"tuple[]"}],"name":"sendCredits","outputs":[{"components":[{"internalType":"uint32","name":"srcEid","type":"uint32"},{"internalType":"uint64","name":"amount","type":"uint64"}],"internalType":"struct Credit[]","name":"","type":"tuple[]"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"components":[{"internalType":"uint32","name":"dstEid","type":"uint32"},{"internalType":"bytes32","name":"to","type":"bytes32"},{"internalType":"uint256","name":"amountLD","type":"uint256"},{"internalType":"uint256","name":"minAmountLD","type":"uint256"},{"internalType":"bytes","name":"extraOptions","type":"bytes"},{"internalType":"bytes","name":"composeMsg","type":"bytes"},{"internalType":"bytes","name":"oftCmd","type":"bytes"}],"internalType":"struct SendParam","name":"_sendParam","type":"tuple"},{"components":[{"internalType":"uint256","name":"nativeFee","type":"uint256"},{"internalType":"uint256","name":"lzTokenFee","type":"uint256"}],"internalType":"struct MessagingFee","name":"_fee","type":"tuple"},{"internalType":"address","name":"_refundAddress","type":"address"}],"name":"sendToken","outputs":[{"components":[{"internalType":"bytes32","name":"guid","type":"bytes32"},{"internalType":"uint64","name":"nonce","type":"uint64"},{"components":[{"internalType":"uint256","name":"nativeFee","type":"uint256"},{"internalType":"uint256","name":"lzTokenFee","type":"uint256"}],"internalType":"struct MessagingFee","name":"fee","type":"tuple"}],"internalType":"struct MessagingReceipt","name":"msgReceipt","type":"tuple"},{"components":[{"internalType":"uint256","name":"amountSentLD","type":"uint256"},{"internalType":"uint256","name":"amountReceivedLD","type":"uint256"}],"internalType":"struct OFTReceipt","name":"oftReceipt","type":"tuple"},{"components":[{"internalType":"uint72","name":"ticketId","type":"uint72"},{"internalType":"bytes","name":"passengerBytes","type":"bytes"}],"internalType":"struct Ticket","name":"ticket","type":"tuple"}],"stateMutability":"payable","type":"function"},{"inputs":[{"components":[{"internalType":"address","name":"feeLib","type":"address"},{"internalType":"address","name":"planner","type":"address"},{"internalType":"address","name":"treasurer","type":"address"},{"internalType":"address","name":"tokenMessaging","type":"address"},{"internalType":"address","name":"creditMessaging","type":"address"},{"internalType":"address","name":"lzToken","type":"address"}],"internalType":"struct StargateBase.AddressConfig","name":"_config","type":"tuple"}],"name":"setAddressConfig","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"_deficitOffsetLD","type":"uint256"}],"name":"setDeficitOffset","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint32","name":"_dstEid","type":"uint32"},{"internalType":"bool","name":"_oft","type":"bool"}],"name":"setOFTPath","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bool","name":"_paused","type":"bool"}],"name":"setPause","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"_gasLimit","type":"uint256"}],"name":"setTransferGasLimit","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"sharedDecimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"stargateType","outputs":[{"internalType":"enum StargateType","name":"","type":"uint8"}],"stateMutability":"pure","type":"function"},{"inputs":[],"name":"status","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"token","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"treasuryFee","outputs":[{"internalType":"uint64","name":"","type":"uint64"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"tvl","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bytes32","name":"guid","type":"bytes32"},{"internalType":"uint8","name":"index","type":"uint8"}],"name":"unreceivedTokens","outputs":[{"internalType":"bytes32","name":"hash","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"withdrawPlannerFee","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_to","type":"address"},{"internalType":"uint64","name":"_amountSD","type":"uint64"}],"name":"withdrawTreasuryFee","outputs":[],"stateMutability":"nonpayable","type":"function"},{"stateMutability":"payable","type":"receive"}]"""

MAX_SLIPPAGE = 1

CHAINS_FOR_WITHDRAW = {
    1, # eth
    10, # op
    42161, # arb
    8453, # base
}

CHAINS_FOR_BRIDGE = {
    534352 # scroll
}


class ExchangeRequest:
    def __init__(self):
        self.total_attempts = 15
        self.binance_api_urls = [
            "https://api.binance.com/",
            "https://api1.binance.com/",
            "https://api2.binance.com/",
            "https://api3.binance.com/",
            "https://api4.binance.com/"
        ]

    def get_binance_ticker_price(self, ticker: str):
        url = f"{random.choice(self.binance_api_urls)}api/v3/ticker/price?symbol={ticker}"
        session = requests.Session()
        headers = {
            "Accept": "application/json",

        }

        try:
            response = session.get(url, headers=headers)
            # response.raise_for_status()
            if 'price' in response.json():
                session.close()
                return response.json()['price']
            else:
                session.close()
                return None
        except requests.exceptions.RequestException as e:
            session.close()
            return None

    def get_bybit_ticker_price(self, ticker: str):
        url = f"https://api.bybit.com/v5/market/tickers?category=spot&symbol={ticker}"
        session = requests.Session()
        headers = {
            "Accept": "application/json",

        }

        try:
            response = requests.get(url, headers=headers)
            if 'result' in response.json():
                return response.json()['result']['list'][0]['lastPrice']
            else:
                session.close()
                return None
        except requests.exceptions.RequestException as e:
            session.close()
            return None

    def get_ticker_price(self, ticker: str):
        success = False
        attempts = 0

        while not success and attempts < self.total_attempts:
            attempts += 1
            try:
                price = 0
                if attempts < 8:
                    price = float(self.get_binance_ticker_price(ticker))

                if 8 <= attempts < 16:
                    price = float(self.get_bybit_ticker_price(ticker))

                if price and float(price) > 0:
                    return price

            except Exception as e:
                if attempts >= 16:
                    logger.error(f"Ошибка получения цены актива: {ticker} - {e}")
                    raise Exception
                pass

            time.sleep(2)

class WithdrawToCEX:
    def __init__(self, evm_private_key: str, chain_id: int):
        self.account = Account.from_key(evm_private_key)
        self.total_attempts = 15
        self.wallet_masked = self.masked_wallet()
        self.web3 = self.web3_connect(chain_id)

    def masked_wallet(self):
        try:
            if len(self.account.address) >= 10:
                return f"{self.account.address[:6]}...{self.account.address[-4:]}"
        except:
            return None

    def to_32byte_hex(self, val) -> str:
        return "0x" + "0" * 24 + str(val)[2:]

    def get_rpc(self, chain_id: int):
        eth = [
            "https://1.rpc.thirdweb.com",
            "https://rpc.ankr.com/eth",
            "https://1rpc.io/eth",
            "https://eth.drpc.org",
            "https://0xrpc.io/eth",
            "https://eth.llamarpc.com"
        ]

        arb = [
            "https://42161.rpc.thirdweb.com",
            "https://rpc.ankr.com/arbitrum",
            "https://arbitrum.llamarpc.com",
            "https://arb1.arbitrum.io/rpc",
            "https://1rpc.io/arb",
            "https://arbitrum.blockpi.network/v1/rpc/public",
            "https://arbitrum-one-rpc.publicnode.com",
            "https://arbitrum.meowrpc.com",
            "https://arbitrum.drpc.org"
        ]

        op = [
            "https://10.rpc.thirdweb.com",
            "https://rpc.ankr.com/optimism",
            "https://optimism.llamarpc.com",
            "https://mainnet.optimism.io",
            "https://1rpc.io/op",
            "https://optimism.drpc.org",
            "https://0xrpc.io/op"
        ]

        base = [
            "https://8453.rpc.thirdweb.com",
            "https://rpc.ankr.com/base",
            "https://base.llamarpc.com",
            "https://mainnet.base.org",
            "https://1rpc.io/base",
            "https://base.meowrpc.com",
            "https://base.drpc.org",
            "https://base.lava.build",
            "https://0xrpc.io/base"
        ]

        zksync = [
            "https://324.rpc.thirdweb.com",
            "https://mainnet.era.zksync.io",
            "https://rpc.ankr.com/zksync_era",
            "https://1rpc.io/zksync2-era"
        ]

        scroll = [
            "https://rpc.scroll.io/",
            "https://rpc.ankr.com/scroll"
        ]

        if chain_id == 1:
            return random.choice(eth)

        if chain_id == 10:
            return random.choice(op)

        if chain_id == 42161:
            return random.choice(arb)

        if chain_id == 8453:
            return random.choice(base)

        if chain_id == 324:
            return random.choice(zksync)

        if chain_id == 534352:
            return random.choice(scroll)

    def web3_connect(self, chain_id: [int, str]):
        attempts = 0
        while attempts < self.total_attempts:
            try:
                attempts += 1
                RPC_URL = self.get_rpc(chain_id)
                web3 = Web3(Web3.HTTPProvider(RPC_URL))

                if chain_id == 324 or chain_id == 534352:
                    web3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

                if not web3.is_connected():
                    time.sleep(random.randint(1, 3))
                    web3.provider = None
                else:
                    return web3
            except:
                pass

        return None

    def get_bal(self):
        decimals = 18
        balance_wei = self.web3.eth.get_balance(self.web3.to_checksum_address(self.account.address))
        balance = float(balance_wei / (10 ** decimals))
        return balance

    def transfer_native_token_max(self, chain_id: [int, str], TO_ADDRESS: str):
        sender = self.account.address

        success = False
        balance = None
        attempts = 0
        while not success and attempts < self.total_attempts:
            attempts += 1
            try:
                web3 = None
                if self.web3 is None:
                    web3 = self.web3_connect(chain_id)
                else:
                    web3 = self.web3

                decimals = 18
                balance_wei = web3.eth.get_balance(web3.to_checksum_address(sender))
                balance = balance_wei / (10 ** decimals)

                gas_estimate = web3.eth.estimate_gas({
                    "from": web3.to_checksum_address(sender),
                    "to": web3.to_checksum_address(TO_ADDRESS),
                    "value": balance_wei
                })

                latest_block = web3.eth.get_block("latest")
                supports_eip1559 = "baseFeePerGas" in latest_block

                if supports_eip1559:
                    base_fee = latest_block['baseFeePerGas']
                    priority_fee = web3.eth.max_priority_fee
                    max_fee_per_gas = base_fee + priority_fee
                    total_gas_fee = gas_estimate * 2 * max_fee_per_gas
                    max_transferable = balance_wei - total_gas_fee

                    tx = {
                        "to": web3.to_checksum_address(TO_ADDRESS),
                        "from": web3.to_checksum_address(sender),
                        "value": max_transferable,
                        "gas": int(gas_estimate * random.uniform(1.3, 1.5)),
                        "maxFeePerGas": max_fee_per_gas,
                        "maxPriorityFeePerGas": priority_fee,
                        "nonce": web3.eth.get_transaction_count(web3.to_checksum_address(sender)),
                        "chainId": web3.eth.chain_id,
                        "type": 2
                    }

                else:
                    gas_price = web3.eth.gas_price
                    total_gas_fee = gas_estimate * gas_price * 2
                    max_transferable = balance_wei - total_gas_fee

                    tx = {
                        "to": web3.to_checksum_address(TO_ADDRESS),
                        "from": web3.to_checksum_address(sender),
                        "value": max_transferable,
                        "gas": int(gas_estimate * random.uniform(1.3, 1.5)),
                        "gasPrice": int(gas_price * random.uniform(1.1, 1.5)),
                        "nonce": web3.eth.get_transaction_count(web3.to_checksum_address(sender)),
                        "chainId": web3.eth.chain_id,
                    }

                if max_transferable > 0:
                    signed_tx = web3.eth.account.sign_transaction(tx, self.account.key)
                    tx_hash = web3.eth.send_raw_transaction(signed_tx.raw_transaction)
                    tx = web3.to_hex(tx_hash)
                    logger.warning(f"[ {self.wallet_masked} ] | "
                                   f"Отправка средств с кошелька на адрес в сети {chain_id}: отправили {web3.from_wei(max_transferable, 'ether'):.8f} ETH. TX: {tx}")
                    return tx

                else:
                    logger.error(f"[ {self.wallet_masked} ] | {chain_id} | После расчета оплаты за газ сумма к отправке меньше или равна 0")
                    return None

            except Exception as e:
                if attempts == self.total_attempts - 2:
                    logger.error(f"[ {self.wallet_masked} ] | Отправка средств с кошелька на адрес в сети {chain_id} - ошибка: {str(e)}")
                    return None
                self.web3 = None
                pass

            time.sleep(2)

    def get_gas_params(self) -> Dict[str, int]:
        latest_block = self.web3.eth.get_block("latest")
        base_fee = latest_block["baseFeePerGas"]
        max_priority_fee = self.web3.eth.max_priority_fee

        max_fee = base_fee + max_priority_fee

        return {
            "maxFeePerGas": max_fee,
            "maxPriorityFeePerGas": max_priority_fee,
        }

    def stargate_bridge_v2(self, chain_from: int, chain_to: int, amount: Optional[float] = 0, mode: Optional[str] = None):
        STARGATE_ETH_NATIVE_POOL_ADDRESSES = {
            42161: "0xA45B5130f36CDcA45667738e2a258AB09f4A5f7F",
            10: "0xe8CDF27AcD73a434D661C84887215F7598e7d0d3",
            8453: "0xdc181Bd607330aeeBEF6ea62e03e5e1Fb4B6F7C7",
            130: "0xe9aBA835f813ca05E50A6C0ce65D0D74390F7dE7",
            534352: "0xC2b638Cb5042c1B3c5d5C969361fB50569840583",
            324: "0xDAc7479e5F7c01CC59bbF7c1C4EDF5604ADA1FF2"
        }

        LZ_CHAIN_EID = {
            42161: 30110,
            10: 30111,
            8453: 30184,
            130: 30320,
            534352: 30214
        }

        # if amount == 0 : MAX AMOUNT of BALANCE
        # else : amount

        sender = self.account.address

        success = False
        balance = None
        attempts = 0
        while not success and attempts < self.total_attempts:
            attempts += 1
            try:
                web3 = None
                if self.web3 is None:
                    self.web3 = self.web3_connect(chain_from)
                    web3 = self.web3
                else:
                    web3 = self.web3

                stargate_address_contract = web3.to_checksum_address(STARGATE_ETH_NATIVE_POOL_ADDRESSES[chain_from])

                stargate_contract = web3.eth.contract(
                    address=stargate_address_contract,
                    abi=json.loads(stargate_native_pool_abi)
                )

                oft_cmd = "0x01" if mode == "BUS" else "0x"

                latest_block = web3.eth.get_block("latest")
                base_fee = latest_block["baseFeePerGas"]
                max_priority_fee = web3.eth.max_priority_fee
                max_fee = base_fee + max_priority_fee



                bridge_amount = int(0)
                minimal_received = int(0)

                if amount == 0:
                    bal = self.get_bal()
                    balance_wei = int(Decimal(str(bal)) * 10**18)

                    _amount_wei = int(Decimal(str(0.0001)) * 10 ** 18)
                    _min_recieved = Web3.to_wei(Decimal(str(0.0001)) * (100 - MAX_SLIPPAGE) / 100, 'ether')

                    _send_param = (
                        int(LZ_CHAIN_EID[chain_to]),
                        self.to_32byte_hex(self.account.address),
                        _amount_wei,
                        _min_recieved,
                        "0x",
                        "0x",
                        oft_cmd
                    )
                    _message_fee = stargate_contract.functions.quoteSend(_send_param, False).call()
                    _value = _amount_wei + _message_fee[0]

                    _data = stargate_contract.encode_abi("send", args=(_send_param, _message_fee, sender))
                    _transaction = {
                        "from": self.account.address,
                        "to": web3.to_checksum_address(stargate_address_contract),
                        "data": _data,
                        "value": _value,
                        "chainId": chain_from,
                        "type": 2,
                    }

                    _estimated_gas = int(web3.eth.estimate_gas(_transaction) * random.uniform(1.1, 1.3))

                    total_gas_fee = _estimated_gas * 2 * max_fee
                    max_transferable = balance_wei - total_gas_fee - int(_message_fee[0] * random.uniform(1.3, 1.5))

                    bridge_amount = max_transferable
                    minimal_received = int(max_transferable * (100 - MAX_SLIPPAGE) / 100)

                else:
                    bridge_amount = int(Decimal(str(amount)) * 10**18)
                    minimal_received = Web3.to_wei(Decimal(str(amount)) * (100 - MAX_SLIPPAGE) / 100, 'ether')

                if bridge_amount > 0 and minimal_received > 0:
                    send_param = (
                        int(LZ_CHAIN_EID[chain_to]),
                        self.to_32byte_hex(self.account.address),
                        bridge_amount,
                        minimal_received,
                        "0x",
                        "0x",
                        oft_cmd
                    )
                    message_fee = stargate_contract.functions.quoteSend(send_param, False).call()
                    value = bridge_amount + message_fee[0]

                    data = stargate_contract.encode_abi("send", args=(send_param, message_fee, sender))
                    transaction = {
                        "from": self.account.address,
                        "to": web3.to_checksum_address(stargate_address_contract),
                        "data": data,
                        "value": value,
                        "chainId": chain_from,
                        "type": 2,
                    }
                    estimated_gas = int(web3.eth.estimate_gas(transaction) * random.uniform(1.1, 1.3))

                    latest_block = web3.eth.get_block("latest")
                    base_fee = latest_block["baseFeePerGas"]
                    max_priority_fee = web3.eth.max_priority_fee
                    max_fee = base_fee + max_priority_fee

                    transaction.update(
                        {
                            "nonce": web3.eth.get_transaction_count(self.account.address, "latest", ),
                            "gas": int(estimated_gas),
                            "maxFeePerGas": int(max_fee),
                            "maxPriorityFeePerGas": int(max_priority_fee)
                        }
                    )

                    signed_tx = web3.eth.account.sign_transaction(transaction, self.account.key)

                    if not hasattr(signed_tx, "raw_transaction"):
                        raise ValueError("signed_tx не содержит raw_transaction")

                    if not isinstance(web3, Web3):
                        raise ValueError("web3 не является объектом Web3")

                    tx_hash = web3.eth.send_raw_transaction(signed_tx.raw_transaction)
                    tx_id = Web3.to_hex(tx_hash)
                    logger.warning(f"[ {self.wallet_masked} ] | "
                                   f"Stargate Bridge (v2) : начали трансфер {(bridge_amount / 10**18):.8f} ETH / {chain_from} => {chain_to} | TX: {tx_id} ")

                    return tx_id



            except Exception as e:
                if attempts == self.total_attempts - 2:
                    logger.error(f"[ {self.wallet_masked} ] | Stargate Bridge (v2) {chain_from} => {chain_to} - ошибка: {str(e)}")
                    return None
                self.web3 = None
                pass

            time.sleep(2)

def log_formatted(source):
    if isinstance(source, list):
        formatted_string = "\n".join(source)
    else:
        formatted_string = source
    now = datetime.now()
    formatted_time = now.strftime("%d/%m/%Y %H:%M")
    return formatted_time + "\n" + formatted_string

def process_wallet(account_data):
    number_wallet = account_data["NUMBER_WALLET"]
    wallet_seed = account_data["SEED"]
    address_to_withdraw = account_data["ADDRESS_TO"]
    wallet_status = account_data["STATUS"]

    mnemo = Mnemonic("english")
    seed = mnemo.to_seed(wallet_seed)

    root_key = BIP32Key.fromEntropy(seed)
    child_key = root_key.ChildKey(44 + BIP32_HARDEN).ChildKey(60 + BIP32_HARDEN).ChildKey(
        0 + BIP32_HARDEN).ChildKey(0).ChildKey(0)

    private_key_bytes = child_key.PrivateKey()
    private_key_hex = private_key_bytes.hex()

    public_key_bytes = keys.PrivateKey(bytes.fromhex(private_key_hex)).public_key
    wallet_address = Web3.to_checksum_address(public_key_bytes.to_checksum_address())

    eth_price = ExchangeRequest().get_ticker_price("ETHUSDT")

    NEW_CHAINS_FOR_WITHDRAW = {chain for chain in CHAINS_FOR_WITHDRAW if chain != 1}

    _BALANCES = {}
    for chain in NEW_CHAINS_FOR_WITHDRAW:
        balance = WithdrawToCEX(private_key_hex, chain).get_bal() * eth_price
        if balance > MIN_USD_AMOUNT:
            _BALANCES[chain] = balance

    _BRIDGES = {}

    for chain in CHAINS_FOR_BRIDGE:
        balance = WithdrawToCEX(private_key_hex, chain).get_bal() * eth_price
        if balance > MIN_USD_AMOUNT:
            _BRIDGES[chain] = balance

    history = []

    if len(_BRIDGES) > 0:
        random_chain_to = 0
        if len(_BALANCES) > 0:
            random_chain_to = max(_BALANCES, key=_BALANCES.get)
        else:
            random_chain_to = random.choice(list(NEW_CHAINS_FOR_WITHDRAW))

        _to_wd = WithdrawToCEX(private_key_hex, random_chain_to)
        for chain_from in _BRIDGES:
            _to_start_balance = _to_wd.get_bal()
            bridge_tx = WithdrawToCEX(private_key_hex, chain_from).stargate_bridge_v2(chain_from, random_chain_to)

            if bridge_tx is not None:
                success_bridge = False
                attempts = 0
                pause_wait = 20
                max_wait = 15 # minutes
                max_attempts = (60 / pause_wait) * max_wait
                while not success_bridge and attempts < max_attempts:
                    attempts += 1
                    _to_end_balance = _to_wd.get_bal()

                    if _to_end_balance > _to_start_balance:
                        history.append(f"BRIDGE:{chain_from} => {random_chain_to}:SUCCESS")
                        success_bridge = True

                    elif attempts == max_attempts - 2:
                        history.append(f"BRIDGE:{chain_from} => {random_chain_to}:WAIT END")
                        success_bridge = True

                    time.sleep(pause_wait)

            else:
                history.append(f"BRIDGE:{chain_from} => {random_chain_to}:SKIP")

            time.sleep(random.randint(2, 5))

    for chain_from in CHAINS_FOR_WITHDRAW:
        withdraw_tx = WithdrawToCEX(private_key_hex, chain_from).transfer_native_token_max(chain_from, address_to_withdraw)

        if withdraw_tx is not None:
            history.append(f"SEND:{chain_from} => {address_to_withdraw}:SUCCESS")
        else:
            history.append(f"SEND:{chain_from} => {address_to_withdraw}:SKIP")

        time.sleep(random.randint(2, 5))

    write_cell("wallet.xlsx", "STATUS", number_wallet, log_formatted(history))

def main():
    print("======== WEB3 WITHDRAW TO CEX ========")
    print(f"Наши ресурсы:\n"
          f"Telegram-канал: @quantumlab_official\n"
          f"Продукты: @quantum_lab_bot\n\n")

    
    account_file_name = "wallet.xlsx"
    accounts_for_work = get_profile_for_work(account_file_name)

    if len(accounts_for_work) > 0:
        logger.warning(f"Запуск активности в {MAX_THREADS} потоков")
        logger.warning(f"В работу отправлено: {len(accounts_for_work)} аккаунтов")

        random.shuffle(accounts_for_work)

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            futures = []
            for account_data in accounts_for_work:
                futures.append(executor.submit(process_wallet, account_data))
                time.sleep(1)

            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Ошибка: {e}")
                    logger.error(traceback.format_exc())

    else:
        logger.warning(f"❌ Нет аккаунтов для работы! Проверьте данные в таблице!")

    print(f"Наши ресурсы:\n"
          f"Telegram-канал: @quantumlab_official\n"
          f"Продукты: @quantum_lab_bot\n\n")
    print("======== WEB3 WITHDRAW TO CEX ========")

main()



