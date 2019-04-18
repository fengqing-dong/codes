#!/usr/bin/env python
# coding=utf-8
from collections import Counter
import re
import argparse
import os
import sys
import time


def get_parse():
    parse = argparse.ArgumentParser(description="%(prog)s 对fastq文件中lane进行过滤和统计")
    parse.add_argument("file", help="输入文件")
    parse.add_argument("-o", "--output", dest="output", action="append", help="输出文件")
    parse.add_argument(
        "-l", "--lanes", dest="lanes", action="append", help="待过滤的lane/tile编号"
    )
    parse.add_argument(
        "-t", "--threads", dest="threads", type=int, default=10, help="多进程处理"
    )
    parse.add_argument(
        "-s", "--site", dest="site", type=int, default=0, help="指定删除节点,在节点之前的序列全部截除"
    )
    parse.add_argument(
        "-r", "--row", dest="row", type=int, default=1024 * 1024, help="指定输出temp文件中行数"
    )
    parse.add_argument(
        "-d", "--deleted", dest="deleted", action="store_false", help="删除整个lane/lane"
    )
    arg = parse.parse_args()
    return arg


class SplitFiles:
    def __init__(self, args):
        self.filename = args.file
        self.output = args.output
        self.threads = args.threads
        self.size = args.row  # 拆分后文件的行数
        self.temp_file_list = list()  # 拆分后的文件名列表
        if not self.filename:
            raise FileNotFoundError("请指定待处理文件!\n")
        if not self.output:
            self.output = "result" + self.filename

    def split_file(self):
        """
        拆分大文件至小文件，可指定拆分个数
        """
        if not os.path.exists(self.filename):
            raise FileNotFoundError("{}文件不存在，请检查正确路径或者命名!\n".format(self.filename))
        with open(self.filename) as f:
            self.lines = sum(1 for _ in f)
            self.split_file_num = self.lines / self.size
            print("{}总行数为:{:d}".format(self.filename, self.lines))
        with open(self.filename) as f:
            store_num = 0
            file_num = 1
            _output_temp_name = "temp{:03d}".format(file_num)
            file_temp_output = open(_output_temp_name, "w")
            self.temp_file_list.append(_output_temp_name)
            for line in f:
                if store_num < self.size:
                    file_temp_output.write(line)
                    store_num += 1
                else:
                    store_num = 1  # 下方写入一行，所以从1开始计算而不是0
                    file_num += 1
                    file_temp_output.close()
                    _output_temp_name = "temp{:03d}".format(file_num)
                    file_temp_output = open(_output_temp_name, "w")
                    file_temp_output.write(line)  # 在新闻文件中写入第一行，千万不能漏
                    self.temp_file_list.append(_output_temp_name)
            file_temp_output.close()
        print("{}文件分割完成，共分割成{}个文件".format(self.filename, len(self.temp_file_list)))
        return self.temp_file_list


class FileHandle(SplitFiles):
    def __init__(self, args):
        SplitFiles.__init__(self, args)
        if not self.temp_file_list:
            self.temp_file_list = [_ for _ in os.listdir() if "temp" in _]
        self.trime_file_list = list()
        self.deleted = args.deleted
        self.site = args.site
        self.lanes = args.lanes
        if not self.lanes:
            raise IndexError("请指定待过滤的tile/lane编号！\n")

    def file_handle_multi(self):
        """
        调度程序，多线程
        """
        # TODO 多线程
        for files in self.temp_file_list:
            self.trim_within_file(files)
        self.merge_file()
        # todo function

    def trim_within_file(self, files):
        """
        对由SRA转换成FASTQ文件，按照tile编号进行裁减。
        self.deleted 参数控制是否删除整个lane|tile，默认不删除        
        """
        print("正在裁剪{}".format(files))
        if not os.path.exists(files):
            raise FileNotFoundError("{}文件不存在，请检查正确路径或者命名!\n".format(files))
        temp_trim_file = "trim_{}".format(files)
        self.trime_file_list.append(temp_trim_file)
        temp_trim_file = open("trim_{}".format(files), "w")
        with open(files, "r") as f:
            for line in f:
                if line[0] == "@" and line.split(":")[-3] in self.lanes:
                    # TODO 从尾部、或者两端裁减
                    if self.deleted:
                        if self.site == 0:
                            print("并未执行裁减，请根据需要指定-s/--site 参数")
                        temp_trim_file.write(line)
                        temp_trim_file.write(
                            " " * (self.site - 1) + f.__next__()[self.site :]
                        )  # Todo 检查site是否超过line长度
                        temp_trim_file.write(f.__next__())
                        temp_trim_file.write(f.__next__()[self.site :])
                    else:
                        f.__next__()
                        f.__next__()
                        f.__next__()
                else:
                    temp_trim_file.write(line)
                    temp_trim_file.write(f.__next__())  # Todo 检查site是否超过line长度
                    temp_trim_file.write(f.__next__())
                    temp_trim_file.write(f.__next__())
        temp_trim_file.close()
        os.remove(files)

    def merge_file(self):
        """
        合并文件
        """
        result = open("trim_{}".format(self.filename), "w")
        if not self.trime_file_list:
            self.trime_file_list = [_ for _ in os.listdir() if "trim" in _]
        print("正在合并文件，共{}文件待合并".format(len(self.trime_file_list)))
        for _file in self.trime_file_list:
            print("正在合并{}".format(_file))
            with open(_file, "r") as f:
                for line in f:
                    result.write(line)
            os.remove(_file)
        result.close()
        print("文件合并完毕")

    def count_chr_length(self):
        """
        计算每条染色体中ATCG的个数，并计算GC%
        """
        file = self.filename
        if not os.path.exists(file):
            raise FileExistsError("{} don't exist in {}".format(file, os.getcwd()))
        chr_length = dict()
        with open(file, "r") as f:
            for line in f:
                if ">" in line:
                    chr_name = line.strip()[1:]
                    chr_length[chr_name] = 0
                else:
                    chr_length[chr_name] += len(line)
        output_name = "chr_length.txt"
        out = open(output_name, "w")
        for key, value in chr_length.items():
            out.write("{} => {}\n".format(key, value))
        out.close()
        return chr_length

    def count_GC_content(self):
        """
        统计fasta|fastq文件中每个染色体|序列的GC%含量。
        """
        total_gc = Counter()
        chr_gc = dict()
        with open(self.filename, "r") as f:
            for line in f:
                if ">" in line:
                    chr_name = line.strip()[1:]
                    chr_gc[chr_name] = Counter()
                else:
                    line = line.upper().strip()
                    chr_gc[chr_name].update(line)
                    total_gc.update(line)
        output_name = "GC_content.txt"
        out = open(output_name, "w")
        out.write("chromose\tA\tT\tG\tC\tN\tpercent\n")
        for key, value in chr_gc.items():
            line = ""
            print(value)
            for base in "ATGCN":
                line += "\t{}".format(str(value[base]))
            gc_percent = (value["G"] + value["C"]) / (
                value["A"] + value["T"] + value["G"] + value["C"]
            )
            gc_percent = round(gc_percent * 100, 2)
            out.write("{}{}\t{}\n".format(key, line, str(gc_percent)))
        total_gc_percent = round(
            (total_gc["G"] + total_gc["C"])
            / (total_gc["A"] + total_gc["T"] + total_gc["G"] + total_gc["C"])
            * 100,
            2,
        )
        line = ""
        for base in "ATGCN":
            line += "\t{}".format(total_gc[base])
        out.write("total{}\t{}\n".format(line, total_gc_percent))
        out.close()
        print("*" * 30)
        print("The total GC content:{}%".format(total_gc_percent))
        print("*" * 30)


if __name__ == "__main__":
    args = get_parse()
    ss = FileHandle(args)
    ss.split_file()
    ss.file_handle_multi()
#    ss.merge_file()
