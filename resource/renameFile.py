#!/bin/python
# 将spark跑出的离散文件重新命名
import os

def mvFun(folderName,outFolder):
    folders = os.listdir(folderName)
    for folder in folders:
        files = os.listdir(folderName+folder)
        for file1 in files:
            if(file1=='part-00000'):
                oldfile = folderName+folder+"/"+file1
                cmd = 'mv '+oldfile+" "+outFolder+folder
                print(cmd)
                os.system(cmd)

if __name__ == '__main__':
    folderName = "/home/zhiyedan/wisetone/make-data/"
    outFolder = "/home/zhiyedan/wisetone/new-data/"
    if not os.path.exists(outFolder):
        os.makedirs(outFolder)
    mvFun(folderName,outFolder)

