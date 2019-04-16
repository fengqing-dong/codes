#!/usr/bin/env python
# coding=utf-8
import re
import argparse
import os
import sys
import time

def get_parse():
    parse = argparse.ArgumentParser(description="%(prog)s 对fastq文件中lane进行过滤和统计")
    parse.add_argument("file",help="输入文件")
    parse.add_argument("-o","--output",dest="output",action="append",
                       help="输出文件")
    parse.add_argument("-l","--lanes",dest="lanes",action="append",
                      help="待过滤的lane/tile编号")
    parse.add_argument("-t","--threads",dest="threads",type=int,default=10,
                      help="多进程处理")
    parse.add_argument("-s","--site",dest="site",type=int,default=0,
                      help="指定删除节点,在节点之前的序列全部截除")
    parse.add_argument("-r","--row",dest="row",type=int,default=1024*1024,
                      help="指定输出temp文件中行数")
    parse.add_argument("-d","--deleted",dest="deleted",action="store_false",
                       help = "删除整个lane/lane")
    arg = parse.parse_args()
    return arg

class SplitFiles():
    def __init__(self,args):
        self.filename = args.file
        self.output   = args.output
        self.lanes    = args.lanes
        self.threads  = args.threads
        self.size     = args.row  # 500M
        self.site     = args.site
        self.temp_file_list = list()
        if not self.filename:
            raise FileNotFoundError("请指定待处理文件!\n")
        if not self.output:
            self.output = "filter" + self.filename
        if not self.lanes:
            raise IndexError("请指定待过滤的tile/lane编号！\n")

    
    def split_file(self):
        if not os.path.exists(self.filename):
            raise FileNotFoundError("{}文件不存在，请检查正确路径或者命名!\n".format(file))
        with open(self.filename) as f:
            self.lines  = sum(1 for _ in f)
            self.split_file_num = self.lines/self.size 
            print("{}总行数为:{:d}".format(self.filename,self.lines))
        with open(self.filename) as f:
            store_num = 0
            file_num = 1
            _output_temp_name = "temp{:03d}".format(file_num)
            file_temp_output = open(_output_temp_name,"w")
            self.temp_file_list.append(_output_temp_name)
            for line in f:
                if store_num < self.size:
                    file_temp_output.write(line)    
                    store_num += 1
                else:
                    store_num = 1   #下方写入一行，所以从1开始计算而不是0
                    file_num +=1
                    file_temp_output.close()
                    _output_temp_name = "temp{:03d}".format(file_num)
                    file_temp_output = open(_output_temp_name,"w")
                    file_temp_output.write(line)   #在新闻文件中写入第一行，千万不能漏
                    self.temp_file_list.append(_output_temp_name)
            file_temp_output.close()
        print("{}文件分割完成，共分割成{}个文件".format(self.filename,len(self.temp_file_list)))

class FileHandle(SplitFiles):
    def __init__(self,args):
        SplitFiles.__init__(self,args)
        if not self.temp_file_list:
            self.temp_file_list = [_ for _ in os.listdir() if "temp" in _]
        self.trime_file_list = list()
        self.deleted = args.deleted 
 
    def file_handle_multi(self):
        for files in self.temp_file_list:
            self.trim_within_file(files)
        self.merge_file()
            # todo function


    def trim_within_file(self,files):
        print("正在裁剪{}".format(files))
        if not os.path.exists(files):
            raise FileNotFoundError("{}文件不存在，请检查正确路径或者命名!\n".format(files))
        temp_trim_file = "trim_{}".format(files)
        self.trime_file_list.append(temp_trim_file)
        temp_trim_file = open("trim_{}".format(files),"w")
        with open(files,"r") as f:
            for line in f:
                if line[0]=="@" and line.split(":")[-3] in self.lanes:
                    if self.deleted:
                        temp_trim_file.write(line)
                        temp_trim_file.write(" "*(self.site-1) + f.__next__()[self.site:])  #Todo 检查site是否超过line长度
                        temp_trim_file.write(f.__next__())
                        temp_trim_file.write(f.__next__()[self.site:])
                    else:
                        f.__next__()
                        f.__next__()
                        f.__next__()
                else:
                    temp_trim_file.write(line)
                    temp_trim_file.write(f.__next__())  #Todo 检查site是否超过line长度
                    temp_trim_file.write(f.__next__())
                    temp_trim_file.write(f.__next__())
        temp_trim_file.close()
        os.remove(files)

    def merge_file(self):
        result = open("trim_{}".format(self.filename),"w")
        if not self.trime_file_list:
            self.trime_file_list = [_ for _ in os.listdir() if "trim" in _]
        print("正在合并文件，共{}文件待合并".format(len(self.trime_file_list)))
        for _file in self.trime_file_list:
            print("正在合并{}".format(_file))
            with open(_file,"r") as f:
                for line in f:
                    result.write(line)
            os.remove(_file)
        result.close()
        print("文件合并完毕")










if __name__ == "__main__":
    args = get_parse()
    ss = FileHandle(args)
    ss.split_file()
    ss.file_handle_multi()
#    ss.merge_file()







