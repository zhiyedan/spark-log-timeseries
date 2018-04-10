#!/usr/bin/python
import os
for month in (2,3):
	for day in range(1,32):
		fileName = 'netLog2016_ZhongXinTong_20180'
		if month==2 and day>28:
			continue
		if day<10 :
			fileName = fileName + str(month) + '0' + str(day) + '_merge.ok'
		else:
			fileName = fileName + str(month) + str(day) + '_merge.ok'
		cmd = 'hdfs dfs -cp netLog2016_ZhongXinTong_20180131_merge.ok '+fileName
		# print(cmd)
		os.system(cmd)