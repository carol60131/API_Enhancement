# -*- coding: utf-8 -*-
# from functions import *
import zipfile
import pandas as pd
import json


def replace(row, UsageAccountId, UsageType, UnblendedRate):
	if row['product/ProductName'] == 'Amazon CloudFront' and row['lineItem/LineItemType'] == 'Usage':
		return UsageAccountId, UsageType, UnblendedRate

	else:
		return pd.Series(row[['lineItem/UsageAccountId', 'lineItem/UsageType', 'lineItem/UnblendedRate']])


def getJson():
	f = open('./data/fix.json')
	data = json.load(f)
	f.close()

	return data


def getData():
	zipFilePath = zipfile.ZipFile('./data/cur.zip')
	file = zipFilePath.open('output.csv')
	df = pd.read_csv(file, dtype={'lineItem/UsageAccountId': 'Int64'}, sep=',')
	pd.set_option("display.max.row", None)

	file.close()
	zipFilePath.close()

	return df


def dataProcessing():
	df = getData()
	fix = getJson()

	for f in fix['detail']:
		df[['lineItem/UsageAccountId', 'lineItem/UsageType', 'lineItem/UnblendedRate']] = df.apply(replace,
			UsageAccountId=f['lineItem/UsageAccountId'], UsageType=f['lineItem/UsageType'],
			UnblendedRate=f['lineItem/UnblendedRate'], axis=1)
		fliter = (df['lineItem/UsageAccountId'] == f['lineItem/UsageAccountId'])
		df = df[fliter]

		compression_opts = dict(method='zip', archive_name=f['lineItem/UsageAccountId'] + '.csv')
		df.to_csv('./output/' + f['lineItem/UsageAccountId'] + '.zip', index=False, compression=compression_opts)


if __name__ == '__main__':
	dataProcessing()
