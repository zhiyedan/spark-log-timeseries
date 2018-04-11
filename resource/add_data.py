#!/usr/bin/python
#coding=utf-8
# hdfs 复制数据，只是名字改变
import os

def geneFileName():
    names = []
    for month in (2, 3):
        for day in range(1, 32):
            fileName = 'netLog2016_ZhongXinTong_20180'
            if month == 2 and day > 28:
                continue
            if day < 10:
                fileName = fileName + str(month) + '0' + str(day) + '_merge.ok'
            else:
                fileName = fileName + str(month) + str(day) + '_merge.ok'
            names.append(fileName)
    return names
                # cmd = 'hdfs dfs -cp netLog2016_ZhongXinTong_20180131_merge.ok '+fileName
                # print(cmd)
                # os.system(cmd)
if __name__ == '__main__':
    fileName = '/home/zhiyedan/wisetone/fileNames.txt'
    names = geneFileName()
    with open(fileName,'w') as file:
        for name in names:
            file.write(name+'\t')