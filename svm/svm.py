#!/usr/bin/env python3

import pandas as pd
# from sklearn.preprocessing import StandardScaler
from sklearn.feature_extraction import DictVectorizer
from sklearn import svm
import numpy as np
import pickle
import os
import requests
# import datetime

import sys
# sys.path.append('..')
dir_path = os.path.dirname(os.path.realpath(__file__))
parent_dir_path = os.path.abspath(os.path.join(dir_path, os.pardir))
sys.path.insert(0, parent_dir_path)
from log import log

projectId = "your project id"
projectSecret = "your project secret"
endpoint = "https://ipfs.infura.io:5001"


def dealCsvData(filePath):
    originData = pd.read_csv(filePath)

    print("*" * 30 + " origin file data info " + "*" * 30)
    log("*" * 30 + " origin file data info " + "*" * 30)
    print(originData.info())
    log(f"{originData.info()}")
    print(originData.head())
    log(f"{originData.head()}")

    print("*" * 30 + " x null handle " + "*" * 30)
    log("*" * 30 + " x null handle " + "*" * 30)

    nullList = originData.isnull().any()
    cols = originData.columns
    for i in range(0, len(nullList)):
        if nullList[i] == True:
            print(cols[i], ' has null')
            log(f"{cols[i]} has null")
            value_mean = originData[cols[i]].mean()
            print(cols[i] + "_mean " + ":")
            print(value_mean)
            log(f"{cols[i]}_mean : {value_mean}")
            originData[cols[i]].fillna(value_mean, inplace=True)
            print(" " * 10 + "*" * 20 + " after " + cols[i] + " null handle " + "*" * 20)
            log(" " * 10 + "*" * 20 + " after " + str(cols[i]) + " null handle " + "*" * 20)

    print(originData.info())
    log(f"{originData.info()}")
    return originData

def onehoEncoded(originData):
    # onehot encoded
    x_dict_list = originData.to_dict(orient='records')
    print("*" * 30 + " train_dict " + "*" * 30)
    log("*" * 30 + " train_dict " + "*" * 30)
    print(pd.Series(x_dict_list[:5]))
    log(f"{pd.Series(x_dict_list[:5])}")

    dict_vec = DictVectorizer()
    originData = dict_vec.fit_transform(x_dict_list)
    print("*" * 30 + " onehot encode " + "*" * 30)
    log("*" * 30 + " onehot encode " + "*" * 30)
    print(originData[:5])
    log(f"{originData[:5]}")
    return originData

def computation(jobId,featurePath, targetPath, testingPath):
    x = dealCsvData(featurePath)
    print("*" * 30 + " x data info " + "*" * 30)
    log("*" * 30 + " x data info " + "*" * 30)
    print(x.info())
    log(f"{x.info()}")
    print(x.head())
    log(f"{x.head()}")

    tradingDataSp = x.shape
    tradingDataComplexity = tradingDataSp[0] * tradingDataSp[1] * 100

    y = pd.read_csv(targetPath)
    print("*" * 30 + "y data info " + "*" * 30)
    log("*" * 30 + "y data info " + "*" * 30)
    print(y.info())
    log(f"{y.info()}")
    print(y.head())
    log(f"{y.head()}")

    x=onehoEncoded(x)

    x_train = x
    y_train = y

    clf = svm.SVC(gamma='scale', C=1.0, decision_function_shape='ovr', kernel='rbf')
    clf.fit(x_train, y_train)

    # save model to file
    folder = jobId
    if not os.path.exists(folder):
        os.mkdir(folder)

    with open(folder + '/model.pickle', 'wb') as handle:
        pickle.dump(clf, handle)  # , protocol=pickle.HIGHEST_PROTOCOL

    modelBytes = pickle.dumps(clf)
    modelComplexity = len(modelBytes) * 100

    if testingPath != "":
        x_test_origin = pd.read_csv(testingPath)

        x_test = dealCsvData(testingPath)
        print("*" * 30 + " x_test data info " + "*" * 30)
        log("*" * 30 + " x_test data info " + "*" * 30)
        print(x_test.info())
        log(f"{x_test.info()}")
        print(x_test.head())
        log(f"{x_test.head()}")

        testDataShape = x_test.shape
        testDataComplexity = testDataShape[0] * testDataShape[1] * 50

        x_test2=onehoEncoded(x_test)
        out = clf.predict(x_test2)
        outDf=pd.DataFrame(data=out,columns=["Result"])
        outDf2=pd.concat([x_test_origin,outDf],axis=1)
        colList = outDf2.columns.tolist()
        colNames=','.join(colList)
        np.savetxt(folder + "/result.csv", outDf2, delimiter=',',fmt = '%s',header=colNames)
    else:
        testDataComplexity = 0

    if testingPath != "":
        ## CREATE AN ARRAY OF TEST FILES ###
        myfiles = {
            'model': open(folder + '/model.pickle', 'rb'),
            'result': open(folder + '/result.csv', 'rb')
        }

        ## ADD FILE TO IPFS AND SAVE THE HASH ###
        response1 = requests.post(endpoint + '/api/v0/add', files=myfiles, auth=(projectId, projectSecret))
        print(response1.text)
        log(f"{response1.text}")
        str2 = response1.text.split("\n")

        dic0 = eval(str2[0])
        modelHash = dic0["Hash"]
        dic1 = eval(str2[1])
        resultHash = dic1["Hash"]
    else:
        ## CREATE AN ARRAY OF TEST FILES ###
        myfiles = {
            'model': open(folder + '/model.pickle', 'rb')
        }

        ## ADD FILE TO IPFS AND SAVE THE HASH ###
        response1 = requests.post(endpoint + '/api/v0/add', files=myfiles, auth=(projectId, projectSecret))
        print(response1.text)
        log(f"{response1.text}")

        str2 = response1.text.split("\n")
        dic0 = eval(str2[0])
        modelHash = dic0["Hash"]
        resultHash = ""

    sumComplexity = tradingDataComplexity + modelComplexity + testDataComplexity

    if testingPath != "":
        msgDict = {
            'model': modelHash,
            'result': resultHash
        }
    else:
        msgDict = {
            'model': modelHash
        }
    return msgDict, sumComplexity