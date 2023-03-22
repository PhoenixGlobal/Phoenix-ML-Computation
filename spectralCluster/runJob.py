#!/usr/bin/env python3
from contract import *
import os
import sys
import datetime
# sys.path.append('..')
dir_path = os.path.dirname(os.path.realpath(__file__))
parent_dir_path = os.path.abspath(os.path.join(dir_path, os.pardir))
sys.path.insert(0, parent_dir_path)
from log import log

from addressApi import getAddressPri,getDatas
from spectralCluster import computation


def startComutation(jobId):
    print("Begin runJob,JobId is ", jobId, ",now time is ", datetime.datetime.now())
    log(f"Begin runJob,JobId is {jobId},now time is {datetime.datetime.now()}")

    # run computation

    featurePath, targetPath, testingPath = getDatas(jobId)
    result, computationComplexity = computation(jobId, featurePath)

    print('computationComplexity is ', computationComplexity, 'result is ', result)
    log(f"computationComplexity is {computationComplexity},result is {result}")
    cost = computationComplexity * 10 ** 10

    submit(result, int(jobId), cost)
    print("End runJob,JobId is ", jobId, ",now time is ", datetime.datetime.now())
    log(f"End runJob,JobId is {jobId},now time is {datetime.datetime.now()}")
    print("---------------------------------------------")
    return


def submit(msg, jobId, ccdCost):
    job = privacyContract.functions.Jobs(jobId).call()
    partyAddress = job[3]
    priKey = getAddressPri(partyAddress)
    if priKey == '':
        print('P0 getAddressPri priKey is empty,return')
        log("P0 getAddressPri priKey is empty,return")
        sys.exit()
    roundId = privacyContract.functions.getRoundId(jobId).call()

    mybyte = str(msg).encode('utf-8')

    nonce = web3.eth.getTransactionCount(partyAddress)
    gasPrice = web3.eth.gasPrice

    transaction = privacyContract.functions.mpcSubmit(jobId, roundId, mybyte, ccdCost).buildTransaction(
        {'gas': 3000000, "gasPrice": gasPrice, 'nonce': nonce})

    signed_txn = web3.eth.account.signTransaction(transaction, priKey)

    txn_hash = web3.eth.sendRawTransaction(signed_txn.rawTransaction)

    msg = f"Transaction successful with hash: {txn_hash.hex()}"
    print(msg)
    log(msg)

theJobId=sys.argv[2]
print('The JobId gotten from sys.argv is ',theJobId)
log(f"The JobId gotten from sys.argv is {theJobId}")
startComutation(theJobId)