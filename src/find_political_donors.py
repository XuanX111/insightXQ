import sys
from pathlib import Path
from statistics import median
from collections import OrderedDict
from datetime import datetime
import cProfile

"""
This class is the data strcut to save the input date from each line
properties:
    cmte_id : CMTE_ID
    zip_code : ZIP_CODE
    trans_dt : TRANSACTION_DT
    trans_amt : TRANSACTION_AMT
"""
class Contribution():
    def __init__(self, cmte_id,zip_code,trans_dt,trans_amt):
        self.cmte_id = cmte_id
        self.zip_code = zip_code
        self.trans_dt = trans_dt
        self.trans_amt = trans_amt

    def __str__(self):
        return self.cmte_id+" "+self.zip_code+" "+self.trans_dt+" "+self.trans_amt

"""
This a base class for recodes
properties:
    cmte_id : CMTE_ID
    count : number of records
    total : total amount
    median : median value
"""
class Record(object):
    def __init__(self, cmte_id,trans_amt):
        self.cmte_id=cmte_id
        self.count = 0
        self.total = 0
        self.median = 0
        self.trans_amts=[]
        self.add_contribution(trans_amt)

    def add_contribution(self,trans_amt):
        """
        when a new contribution add to the recod,
        this function updates the median, count, total
        :param trans_amt:
        :return:
        """
        self.count+=1
        self.trans_amts.append(int(trans_amt))
        self.total+=int(trans_amt)
        self.median=self.get_median()

    def get_median(self):
        return round(median(self.trans_amts))

"""
record class for output to medianvals_by_zip.txt
"""
class Record_zip(Record):
    def __init__(self, cmte_id, zip_code, trans_amt):
        self.zip_code = zip_code
        super(Record_zip,self).__init__(cmte_id,trans_amt)

"""
record class for output to medianvals_by_date.txt
"""
class Record_date(Record):
    def __init__(self, cmte_id, trans_dt, trans_amt):
        self.trans_dt = trans_dt
        super(Record_date, self).__init__(cmte_id, trans_amt)

"""
This is main class to process the data

Properties:
    output_zip is a nested hashmap: (CMTE_ID,(ZIP_CODE,record_zip))
    output_date is a nested hashmap: (CMTE_ID,(Date,record_date))

Attributes:
    process_data(): entre function to process data

    parse_line(line): parse and filter the data of each line

    update_output_zip(Contribution): update the output_zip and calculate the median, count and total

    update_output_date(Contribution): update the output_date and calculate the median, count and total

    write_zip_output(File, Contribution): streaming output for medianvals_by_zip.txt

    write_date_output(): output for medianvals_by_date.txt
"""
class find_political_donors():
    # This is a static map used for describing FEC data dictionary.
    # This map only inlcudes the 5 tuples that we need. But can be easily expanded.
    __parse_index = {"cmte_id": 0, \
                     "zip_code": 10, \
                     "trans_dt": 13, \
                     "trans_amt": 14, \
                     "other_id": 15}

    def __init__(self, input_file, output_file_zip, output_file_date):
        self.input_file = input_file
        self.output_file_zip = output_file_zip
        self.output_file_date = output_file_date
        self.output_zip = {}
        self.output_date = {}

    def process_data(self):
        print ("Start processing data!!!")
        if not self.is_valid_file(self.input_file):
            print("please select a valid input file path!!!")
            return
        with open(self.input_file, 'r') as in_file, \
                open (self.output_file_zip,'w') as zipout_file:
            for line in in_file:
                contrib=self.parse_line(line)
                #print (contrib)
                if not contrib is None:
                    self.update_output(contrib)
                    self.write_zip_output(zipout_file,contrib)
                #else:
                    #print("bad data, skip this line")
        self.write_date_output()
        print("Finished processing data!!!")

    def parse_line(self,line):
        parts = line.split('|')
        if not self.is_valid_line(parts):
            return
        other_id = self.get_other_id(parts)
        if other_id is not '':
            return
        cmte_id = self.get_cmte_id(parts)
        trans_amt = self.get_trans_amt(parts)
        if not cmte_id or not trans_amt :  # ignore lineswith empty CMTE_ID OR TRANSACTION_AMT
            return
        zip_code = self.get_zip_code(parts)
        trans_dt = self.get_trans_dt(parts)
        zip_code = zip_code[0:5]
        return Contribution(cmte_id,zip_code,trans_dt,trans_amt)

    def get_other_id(self,parts):
        return parts[self.__parse_index["other_id"]]

    def get_cmte_id(self,parts):
        return parts[self.__parse_index["cmte_id"]]

    def get_trans_amt(self,parts):
        return parts[self.__parse_index["trans_amt"]]

    def get_trans_dt(self,parts):
        return parts[self.__parse_index["trans_dt"]]

    def get_zip_code(self,parts):
        return parts[self.__parse_index["zip_code"]]

    def update_output(self,contrib):
        self.update_output_zip(contrib)
        self.update_output_date(contrib)

    def update_output_zip(self,contrib):
        if self.is_valid_zipcode(contrib.zip_code):
            if contrib.cmte_id not in self.output_zip:
                self.output_zip[contrib.cmte_id]={}
            if contrib.zip_code not in self.output_zip[contrib.cmte_id]:
                self.output_zip[contrib.cmte_id][contrib.zip_code]\
                    =Record_zip(contrib.cmte_id,contrib.zip_code,contrib.trans_amt)
            else:
                self.output_zip[contrib.cmte_id][contrib.zip_code]\
                    .add_contribution(contrib.trans_amt)

    def update_output_date(self, contrib):
        date = self.get_valid_transdt(contrib.trans_dt)
        if date is not None:
            if contrib.cmte_id not in self.output_date:
                self.output_date[contrib.cmte_id]={}
            if date not in self.output_date[contrib.cmte_id]:
                self.output_date[contrib.cmte_id][date]\
                    =Record_date(contrib.cmte_id,contrib.trans_dt,contrib.trans_amt)
            else:
                self.output_date[contrib.cmte_id][date]\
                    .add_contribution(contrib.trans_amt)

    def write_zip_output(self,f,contrib):
        if self.is_valid_zipcode(contrib.zip_code):
            rcd=self.output_zip[contrib.cmte_id][contrib.zip_code]
            temp = [rcd.cmte_id, rcd.zip_code, str(rcd.median),str(rcd.count),str (rcd.total)]
            out_data = '|'.join(temp)
            f.write(out_data + '\n')

    def write_date_output(self):
        with open(self.output_file_date,'a') as dateout_file:
            d = OrderedDict(sorted(self.output_date.items()))  # sorts the outer keys
            for k, v in d.items():
                d[k] = OrderedDict(sorted(v.items(), key=lambda x: x[0] ))
            for k, v in d.items():
                for x, rcd in v.items():
                    temp = [rcd.cmte_id, rcd.trans_dt, str(rcd.median), str(rcd.count),
                            str(rcd.total)]
                    out_data = '|'.join(temp)
                    dateout_file.write(out_data + '\n')

    def get_valid_transdt(self,trans_dt):
        try:
            date=datetime.strptime(trans_dt, '%m%d%Y')
        except ValueError:
            return None
        return date

    def is_valid_zipcode(self,zip_code):
        return len(zip_code) == 5

    def is_valid_file(self,path):
        my_file = Path(path)
        return my_file.is_file() and my_file.exists()

    def is_valid_line(self,parts):
        return len(parts)==21



if __name__ == "__main__":

    if len(sys.argv)!=4:
        print("Usage: ./find_political_donors input_file output_file_zip output_file_date")
        exit();
    else:
        input_file = sys.argv[1]
        output_file_zip = sys.argv[2]
        output_file_date = sys.argv[3]
        start = datetime.now()
        find_political_donors(input_file,output_file_zip,output_file_date).process_data()
        #cProfile.run("find_political_donors(filename).process_data()")
        end = datetime.now()
        print(end-start)


