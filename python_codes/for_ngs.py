#!/usr/bin/env python
# coding=utf-8
from collections import Counter
import os
import argparse
import re


def get_arg():
    parser = argparse.ArgumentParser(description="%(prog)s 收录一些NGS分析小工具")
    parser.add_argument('filename', metavar="filename",nargs='+',help='输入(待处理)文件名')
    parser.add_argument('-o','--output', dest="output_filename",help='输出文件名')
        
    args = parser.parse_args()
    print(args)
    return args

def count_chr_length(args):
    if not args.filename:
        raise IOError("No file was assigned!")
    for file in args.filename:
        if not os.path.exists(file):
            raise IOError("{} don't exist in {}".format(file, os.getcwd()))
    file = args.filename[0]
    chr_length = dict()
    with open(file, "r") as f:
        for line in f:
            if ">" in line:
                chr_name = line.strip()[1:]
                chr_length[chr_name]=0
            else:
                chr_length[chr_name] += len(line)
    
#    print(chr_length)
    output_name = "chr_length.txt"
    if args.output_filename:
        output_name = args.output_filename[0]
    out = open(output_name,"w")
    for key, value in chr_length.items():
        out.write("{} => {}\n".format(key,value))
    out.close()
    return chr_length

def count_GC_content(args):
    file = args.filename[0]
    total_gc = Counter()
    chr_gc = dict()
    with open(file,"r") as f:
        for line in f:
            if ">" in line:
                chr_name=line.strip()[1:]
                chr_gc[chr_name]=Counter()
            else:
                line=line.upper().strip()
                chr_gc[chr_name].update(line)
                total_gc.update(line)
    output_name = "GC_content.txt"
    if args.output_filename:
        output_name = args.output_filename[0]
    out = open(output_name,"w")
    out.write("chromose\tA\tT\tG\tC\tpercent\n")
    for key, value in chr_gc.items():
        line = ""
        print(value)
        for base in 'ATGCN':
            line += "\t{}".format(str(value[base]))
        gc_percent = (value["G"]+value["C"])/(value["A"]+value["T"]+value["G"]+value["C"])
        gc_percent = round(gc_percent*100,2)
        
        out.write("{}{}\t{}\n".format(key,line,str(gc_percent)))
    total_gc_percent = round((total_gc["G"] + total_gc["C"])/(total_gc["A"] + total_gc["T"]+total_gc["G"]+ total_gc["C"])*100,2)
    line=""
    for base in "ATGCN":
        line += "\t{}".format(total_gc[base])
    out.write("total{}\t{}\n".format(line, total_gc_percent))
    out.close()
    print("*"*30)
    print("The total GC content:{}%".format(total_gc_percent))
    print("*"*30)



def file_split(args):
    file = args.filename[0]

    



    pass





args = get_arg()
#count_chr_length(args)
count_GC_content(args)
