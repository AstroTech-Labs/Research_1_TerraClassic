import aiohttp
import json
import asyncio
import numpy as np
import pandas as pd
import time


try:
    staking_txs = pd.read_csv("./txs_data/staking_txs.csv")
except:
    staking_txs = pd.DataFrame(
        [], columns=['TxHash', 'BlockHeight', 'Sender', 'TxType'])

try:
    slashing_txs = pd.read_csv("./txs_data/slashing_txs.csv")
except:
    slashing_txs = pd.DataFrame(
        [], columns=['TxHash', 'BlockHeight', 'Sender', 'TxType'])

try:
    ibc_client_txs = pd.read_csv("./txs_data/ibc_client_txs.csv")
except:
    ibc_client_txs = pd.DataFrame(
        [], columns=['TxHash', 'BlockHeight', 'Sender', 'TxType'])

try:
    ibc_channel_txs = pd.read_csv("./txs_data/ibc_channel_txs.csv")
except:
    ibc_channel_txs = pd.DataFrame(
        [], columns=['TxHash', 'BlockHeight', 'Sender', 'TxType'])

try:
    ibc_connection_txs = pd.read_csv("./txs_data/ibc_connection_txs.csv")
except:
    ibc_connection_txs = pd.DataFrame(
        [], columns=['TxHash', 'BlockHeight', 'Sender', 'TxType'])

try:
    distribution_txs = pd.read_csv("./txs_data/distribution_txs.csv")
except:
    distribution_txs = pd.DataFrame(
        [], columns=['TxHash', 'BlockHeight', 'Sender', 'TxType'])

try:
    crisis_txs = pd.read_csv("./txs_data/crisis_txs.csv")
except:
    crisis_txs = pd.DataFrame(
        [], columns=['TxHash', 'BlockHeight', 'Sender', 'TxType'])

try:
    evidence_txs = pd.read_csv("./txs_data/evidence_txs.csv")
except:
    evidence_txs = pd.DataFrame(
        [], columns=['TxHash', 'BlockHeight', 'Sender', 'TxType'])

try:
    feegrant_txs = pd.read_csv("./txs_data/feegrant_txs.csv")
except:
    feegrant_txs = pd.DataFrame(
        [], columns=['TxHash', 'BlockHeight', 'Sender', 'TxType'])

try:
    gov_txs = pd.read_csv("./txs_data/gov_txs.csv")
except:
    gov_txs = pd.DataFrame(
        [], columns=['TxHash', 'BlockHeight', 'Sender', 'TxType'])

try:
    upgrade_txs = pd.read_csv("./txs_data/upgrade_txs.csv")
except:
    upgrade_txs = pd.DataFrame(
        [], columns=['TxHash', 'BlockHeight', 'Sender', 'TxType'])

try:
    params_txs = pd.read_csv("./txs_data/params_txs.csv")
except:
    params_txs = pd.DataFrame(
        [], columns=['TxHash', 'BlockHeight', 'Sender', 'TxType'])

try:
    gov_vote_txs = pd.read_csv("./txs_data/gov_vote_txs.csv")
except:
    gov_vote_txs = pd.DataFrame(
        [], columns=['TxHash', 'BlockHeight', 'voter', 'option', 'proposal_id'])

try:
    exchange_rate_vote_txs = pd.read_csv(
        "./txs_data/exchange_rate_vote_txs.csv")
except:
    exchange_rate_vote_txs = pd.DataFrame(
        [], columns=['TxHash', 'BlockHeight', 'salt', 'feeder', 'validator', 'exchange_rates'])

try:
    unidentified_txs = pd.read_csv("./txs_data/unidentified_txs.csv")
except:
    unidentified_txs = pd.DataFrame(
        [], columns=['TxHash', 'BlockHeight', 'Sender', 'TxType'])

try:
    market_swap_txs = pd.read_csv("./txs_data/market_swap_txs.csv")
except:
    market_swap_txs = pd.DataFrame(
        [], columns=['TxHash', 'BlockHeight', 'Sender', 'ask_denom', 'burner', 'tokens_burnt', 'minter', 'tokens_minted', 'offer_coin', 'trader', 'recipient', 'swap_coin', 'swap_fee'])

try:
    market_swap_send_txs = pd.read_csv(
        "./txs_data/market_swap_send_txs.csv")
except:
    market_swap_send_txs = pd.DataFrame(
        [], columns=['TxHash', 'BlockHeight', 'Sender', 'ask_denom', 'burner', 'tokens_burnt', 'minter', 'tokens_minted', 'offer_coin', 'trader', 'recipient', 'swap_coin', 'swap_fee'])


async def get_historical_market_swap_txs():

    offset = 285487736  # But was halted a second time at block 7607789
    session = aiohttp.ClientSession()
    while(1):
        tasks = []

        for i in range(0, 30):
            task = asyncio.ensure_future(fetch_data(session, offset))
            tasks.append(task)
            offset = offset - 100
            # print(offset)

        responses = await asyncio.gather(*tasks, return_exceptions=True)
        exceptions = [res for res in responses if isinstance(res, Exception)]
        successful_results = [
            res for res in responses if not isinstance(res, Exception)]

        print(f'fetch_data -::- Finished successfully: {successful_results}')
        print(f'fetch_data -::- Threw exceptions: {exceptions}')

        exchange_rate_vote_txs.to_csv(
            f'./txs_data/exchange_rate_vote_txs.csv', index=False)
        staking_txs.to_csv(
            f'./txs_data/staking_txs.csv', index=False)

        slashing_txs.to_csv(
            f'./txs_data/slashing_txs.csv', index=False)
        ibc_client_txs.to_csv(
            f'./txs_data/ibc_client_txs.csv', index=False)
        ibc_channel_txs.to_csv(
            f'./txs_data/ibc_channel_txs.csv', index=False)
        ibc_connection_txs.to_csv(
            f'./txs_data/ibc_connection_txs.csv', index=False)
        distribution_txs.to_csv(
            f'./txs_data/distribution_txs.csv', index=False)
        crisis_txs.to_csv(
            f'./txs_data/crisis_txs.csv', index=False)
        evidence_txs.to_csv(
            f'./txs_data/evidence_txs.csv', index=False)
        feegrant_txs.to_csv(
            f'./txs_data/feegrant_txs.csv', index=False)
        gov_txs.to_csv(
            f'./txs_data/gov_txs.csv', index=False)
        gov_vote_txs.to_csv(
            f'./txs_data/gov_vote_txs.csv', index=False)
        params_txs.to_csv(
            f'./txs_data/params_txs.csv', index=False)
        upgrade_txs.to_csv(
            f'./txs_data/upgrade_txs.csv', index=False)
        market_swap_txs.to_csv(
            f'./txs_data/market_swap_txs.csv', index=False)
        market_swap_send_txs.to_csv(
            f'./txs_data/market_swap_send_txs.csv', index=False)
        unidentified_txs.to_csv(
            f'./txs_data/unidentified_txs.csv', index=False)
        await asyncio.sleep(15)


async def fetch_data(session, offset):
    try:
        URL = "https://fcd.terra.dev/v1/txs?offset=" + \
            str(offset) + "&limit=100"
        print(URL)
        while(1):
            async with session.get(URL) as response_:
                try:
                    response = await response_.json(content_type=None)
                    break
                except Exception as e:
                    print(e)
                    await asyncio.sleep(90)
                    continue

        for tx in response['txs']:
            try:
                txhash = tx['txhash']
                # print(txhash)
                block_height = tx['height']

                # IF TX FAILED // EXCEEDS BlockHeight
                if not tx.get('logs'):
                    continue

                msg_number = 0
                for msg in tx['tx']['value']['msg']:
                    sender = ""

                    if msg['value'].get('sender') == None:
                        # if trader (swap txs)
                        if msg['value'].get('trader') != None:
                            sender = msg['value']['trader']
                        # if feeder (oracle txs)
                        elif msg['type'] == "oracle/MsgAggregateExchangeRatePrevote" or msg['type'] == "oracle/MsgAggregateExchangeRateVote":
                            sender = msg['value']['feeder']
                        else:
                            pass
                            # print(f"Sender not available = {txhash}")
                    else:
                        sender = msg['value']['sender']

                    # Ignore WASM module Msgs
                    if msg['type'] == "wasm/MsgStoreCode" or msg['type'] == "wasm/MsgMigrateCode" or msg['type'] == "wasm/MsgInstantiateContract" or msg['type'] == "wasm/MsgExecuteContract" or msg['type'] == "wasm/MsgMigrateContract" or msg['type'] == "wasm/MsgUpdateContractAdmin" or msg['type'] == "wasm/MsgClearContractAdmin":
                        pass

                    # Ignore ORACLE module Msgs
                    elif msg['type'] == "oracle/MsgDelegateFeedConsent" or msg['type'] == "oracle/MsgAggregateExchangeRatePrevote":
                        pass

                    elif msg['type'] == "oracle/MsgAggregateExchangeRateVote":

                        exchange_rate_vote_txs.loc[len(exchange_rate_vote_txs.index)] = [
                            txhash, block_height, msg['value']['salt'], msg['value']['feeder'], msg['value']['validator'], msg['value']['exchange_rates']]

                    # Ignore BANK module Msgs (This module is in 'custom' folder)
                    elif msg['type'] == "bank/MsgSend" or msg['type'] == "bank/MsgMultiSend":
                        pass

                    # Ignore STAKING module Msgs (This module is in 'custom' folder)
                    # or msg['type'] == "staking/MsgDelegate" or msg['type'] == "staking/MsgUndelegate" or msg['type'] == "staking/MsgBeginRedelegate" or msg['type'] == "staking/MsgCancelUnbondingDelegation":
                    elif msg['type'] == "staking/MsgCreateValidator" or msg['type'] == "staking/MsgEditValidator":
                        staking_txs.loc[len(staking_txs.index)] = [
                            txhash, block_height, sender, msg['type']]

                    # Ignore SLASHING module Msgs (This module is in 'custom' folder)
                    elif msg['type'] == "slashing/MsgUnjail":
                        slashing_txs.loc[len(slashing_txs.index)] = [
                            txhash, block_height, sender, msg['type']]

                    # Ignore IBC/core/client/ module Msgs  (This module is in 'third_party' folder)
                    elif msg['type'] == "ibc/MsgCreateClient" or msg['type'] == "ibc/MsgUpdateClient" or msg['type'] == "ibc/MsgUpgradeClient" or msg['type'] == "ibc/MsgSubmitMisbehaviour":
                        ibc_client_txs.loc[len(ibc_client_txs.index)] = [
                            txhash, block_height, sender, msg['type']]

                    # Ignore IBC/core/channel module Msgs  (This module is in 'third_party' folder)
                    elif msg['type'] == "ibc/MsgChannelOpenInit" or msg['type'] == "ibc/MsgChannelOpenTry" or msg['type'] == "ibc/MsgChannelOpenAck" \
                        or msg['type'] == "ibc/MsgChannelOpenConfirm" or msg['type'] == "ibc/MsgChannelCloseInit" or msg['type'] == "ibc/MsgChannelCloseConfirm"  \
                            or msg['type'] == "ibc/MsgRecvPacket" or msg['type'] == "ibc/MsgTimeout" or msg['type'] == "ibc/MsgTimeoutOnClose" or msg['type'] == "ibc/MsgAcknowledgement":
                        sender = msg['value']['signer']
                        ibc_channel_txs.loc[len(ibc_channel_txs.index)] = [
                            txhash, block_height, sender, msg['type']]

                    # Ignore IBC/core/connection module Msgs  (This module is in 'third_party' folder)
                    elif msg['type'] == "ibc/MsgConnectionOpenInit" or msg['type'] == "ibc/MsgConnectionOpenTry" or msg['type'] == "ibc/MsgConnectionOpenAck" or msg['type'] == "ibc/MsgConnectionOpenConfirm":
                        ibc_connection_txs.loc[len(ibc_connection_txs.index)] = [
                            txhash, block_height, sender, msg['type']]

                    # Ignore IBC/applications/transfer/ module Msgs  (This module is in 'third_party' folder)
                    elif msg['type'] == "ibc/MsgTransfer" or msg['type'] == "cosmos-sdk/MsgTransfer":
                        pass

                    # Ignore custom/distribution/ module Msgs  (This module is in 'custom' folder)
                    elif msg['type'] == "distribution/MsgWithdrawValidatorCommission" \
                        or msg['type'] == "distribution/MsgModifyWithdrawAddress" or msg['type'] == "distribution/MsgFundCommunityPool" \
                            or msg['type'] == "distribution/CommunityPoolSpendProposal":  # msg['type'] == "distribution/MsgWithdrawDelegationReward" or
                        distribution_txs.loc[len(distribution_txs.index)] = [
                            txhash, block_height, sender, msg['type']]

                    # Ignore custom/crisis/ module Msgs  (This module is in 'custom' folder)
                    elif msg['type'] == "crisis/MsgVerifyInvariant":
                        crisis_txs.loc[len(crisis_txs.index)] = [
                            txhash, block_height, sender, msg['type']]

                    # Ignore custom/evidence/ module Msgs  (This module is in 'custom' folder)
                    elif msg['type'] == "evidence/MsgSubmitEvidence" or msg['type'] == "evidence/Equivocation":
                        evidence_txs.loc[len(evidence_txs.index)] = [
                            txhash, block_height, sender, msg['type']]

                    # Ignore custom/feegrant/ module Msgs  (This module is in 'custom' folder)
                    elif msg['type'] == "feegrant/MsgGrantAllowance" or msg['type'] == "feegrant/MsgRevokeAllowance"  \
                            or msg['type'] == "feegrant/BasicAllowance" or msg['type'] == "feegrant/PeriodicAllowance" \
                            or msg['type'] == "feegrant/AllowedMsgAllowance":
                        feegrant_txs.loc[len(feegrant_txs.index)] = [
                            txhash, block_height, sender, msg['type']]

                    # Ignore custom/gov/ module Msgs  (This module is in 'custom' folder)
                    ##########---------##########----- PROPOSAL RELATED Tx -----##########---------##########
                    elif msg['type'] == "gov/MsgSubmitProposal" or msg['type'] == "gov/MsgDeposit"  \
                            or msg['type'] == "gov/MsgVoteWeighted" \
                            or msg['type'] == "gov/TextProposal":
                        gov_txs.loc[len(gov_txs.index)] = [
                            txhash, block_height, sender, msg['type']]

                    elif msg['type'] == "gov/MsgVote":
                        voter = msg['value']['voter']
                        option = msg['value']['option']
                        proposal_id = msg['value']['proposal_id']
                        gov_vote_txs.loc[len(gov_vote_txs.index)] = [
                            txhash, block_height, voter, option, proposal_id]

                    ##########---------##########----- PARAMETER-CHANGE Tx -----##########---------##########
                    # Ignore custom/params/types/proposal/ module Msgs  (This module is in 'custom' folder)
                    elif msg['type'] == "params/ParameterChangeProposal":
                        params_txs.loc[len(params_txs.index)] = [
                            txhash, block_height, sender, msg['type']]

                    ##########---------##########----- UPGRADE Tx -----##########---------##########
                    # Ignore custom/upgrade/ module Msgs  (This module is in 'custom' folder)
                    elif msg['type'] == "upgrade/Plan" or msg['type'] == "upgrade/SoftwareUpgradeProposal" or msg['type'] == "upgrade/CancelSoftwareUpgradeProposal":
                        upgrade_txs.loc[len(upgrade_txs.index)] = [
                            txhash, block_height, sender, msg['type']]

                    ##########---------##########----- MARKET SWAP Tx -----##########---------##########
                    elif msg['type'] == "market/MsgSwap":
                        ask_denom = msg['value']['ask_denom']
                        burner = ""
                        tokens_burnt = ""
                        minter = ""
                        tokens_minted = ""
                        offer_coin = ""
                        trader = ""
                        recipient = ""
                        swap_coin = ""
                        swap_fee = ""
                        events = tx['logs'][msg_number]['events']
                        for event in events:
                            # Tokens burnt details
                            if event['type'] == "burn":
                                for attribute in event['attributes']:
                                    if attribute['key'] == 'burner':
                                        burner = attribute['value']
                                    if attribute['key'] == 'amount':
                                        tokens_burnt = attribute['value']
                            # Tokens minted details
                            if event['type'] == "coinbase":
                                for attribute in event['attributes']:
                                    if attribute['key'] == 'minter':
                                        minter = attribute['value']
                                    if attribute['key'] == 'amount':
                                        tokens_minted = attribute['value']
                            # Market Swap Details
                            if event['type'] == "swap":
                                for attribute in event['attributes']:
                                    if attribute['key'] == 'offer':
                                        offer_coin = attribute['value']
                                    if attribute['key'] == 'trader':
                                        trader = attribute['value']
                                    if attribute['key'] == 'recipient':
                                        recipient = attribute['value']
                                    if attribute['key'] == 'swap_coin':
                                        swap_coin = attribute['value']
                                    if attribute['key'] == 'swap_fee':
                                        swap_fee = attribute['value']
                        market_swap_txs.loc[len(market_swap_txs.index)] = [
                            txhash, block_height, sender, ask_denom, burner, tokens_burnt, minter, tokens_minted, offer_coin, trader, recipient, swap_coin, swap_fee]

                        # print(f"market/swap + {txhash}")

                    ##########---------##########----- MARKET SWAP-SEND Tx -----##########---------##########
                    elif msg['type'] == "market/MsgSwapSend":
                        ask_denom = msg['value']['ask_denom']
                        burner = ""
                        tokens_burnt = ""
                        minter = ""
                        tokens_minted = ""
                        offer_coin = ""
                        trader = ""
                        recipient = ""
                        swap_coin = ""
                        swap_fee = ""
                        events = tx['logs'][msg_number]['events']
                        for event in events:
                            # Tokens burnt details
                            if event['type'] == "burn":
                                for attribute in event['attributes']:
                                    if attribute['key'] == 'burner':
                                        burner = attribute['value']
                                    if attribute['key'] == 'amount':
                                        tokens_burnt = attribute['value']
                            # Tokens minted details
                            if event['type'] == "coinbase":
                                for attribute in event['attributes']:
                                    if attribute['key'] == 'minter':
                                        minter = attribute['value']
                                    if attribute['key'] == 'amount':
                                        tokens_minted = attribute['value']
                            # Market Swap Details
                            if event['type'] == "swap":
                                for attribute in event['attributes']:
                                    if attribute['key'] == 'offer':
                                        offer_coin = attribute['value']
                                    if attribute['key'] == 'trader':
                                        trader = attribute['value']
                                    if attribute['key'] == 'recipient':
                                        recipient = attribute['value']
                                    if attribute['key'] == 'swap_coin':
                                        swap_coin = attribute['value']
                                    if attribute['key'] == 'swap_fee':
                                        swap_fee = attribute['value']

                        market_swap_send_txs.loc[len(market_swap_send_txs.index)] = [
                            txhash, block_height, sender, ask_denom, burner, tokens_burnt, minter, tokens_minted, offer_coin, trader, recipient, swap_coin, swap_fee]

                        print(f"market/swap_send + {txhash}")

                    elif msg['type'] != "distribution/MsgWithdrawDelegationReward" and msg['type'] != "staking/MsgUndelegate" and msg['type'] != "staking/MsgBeginRedelegate":
                        print(f"UNIDENTIFIED + {txhash}")
                        unidentified_txs.loc[len(unidentified_txs.index)] = [
                            txhash, block_height, sender, msg['type']]

                    msg_number = msg_number + 1

            except Exception as e:
                print(e)

        print(f"offset = {offset} block_height = {block_height}")

    except Exception as e:
        print(e)


asyncio.run(get_historical_market_swap_txs())
# get_historical_market_swap_txs()


# market/MsgSwap 261F3ADC15A184C0B46717B818E517EAC6601EACC94EB442BAC57064CAC849DA
# market/MsgSwap 0305F47A72051AD3214D59374EB4A852B222C70AE6325169D984A50D87DB4696
