
import pyspark
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import Row, SparkSession
import re
import time
from datetime import datetime

sc = SparkContext()
ibope_txt = sc.textFile("/home/pankaj/python_projects/forastar/_temp.txt")

def processFileLines(row):
    global household_id, start_date, end_date, start_time, end_time, region, city, sel, pay_tv, hh_kids, household_weight, indiviadual_code, start_individual, insert_index,i_region, i_city, i_sel, i_pay_tv, gender, years, housewife, head_of_household, individual_weight, canal, i_row, base_canal, tc, list_time, a_idx, b_idx, a,b, start_number
    if(row.startswith('I')):
        start_number = ''
        household_id = row.strip()[:9]
        start_date = row.strip()[9:][:-16]
        start_time = ':'.join(row.strip()[17:][:-12][i:i+2] for i in range(0, len(row.strip()[17:][:-12]), 2))
        end_date = row.strip()[21:][:-4]
        end_time = ':'.join(row.strip()[29:][i:i+2] for i in range(0, len(row.strip()[29:]), 2))
        yield Row(**{})
        
    if((row.startswith('D') and len(row) == 11)):
        region_data = row.split(',')
        region = region_data[1]
        city = region_data[2]
        sel = region_data[3]
        pay_tv = region_data[4]
        hh_kids = region_data[5]
        total_char = len(row)
        yield Row(**{})
   
    if((row.startswith('W'))):
        household_weight = row.strip()[1:]  
        #if(start_number == ''):
        start_number = start_number + row
        yield Row(**{})
        
    if(row.startswith('Z')):
        indiviadual_code = row    
        yield Row(**{})
             
    if((row.startswith('D') and len(row) != 11)):
        region_data = row.split(',')
        i_row = row
        i_region = region_data[1] if len(region_data) >= 2 else ''
        i_city = region_data[2] if len(region_data) >= 3 else ''
        i_sel = region_data[3] if len(region_data) >= 4 else ''
        i_pay_tv = region_data[4] if len(region_data) >= 5 else ''
        gender = region_data[5] if len(region_data) >= 6 else ''
        years = region_data[6] if len(region_data) >= 7 else ''
        housewife = region_data[7] if len(region_data) >= 8 else ''
        head_of_household = region_data[8] if len(region_data) >= 9 else ''
        total_char = len(row)
        yield Row(**{}) 
        
    if((row.startswith('W'))):
        individual_weight = row.strip()[1:]
        start_number = start_number + row
        yield Row(**{})
    
    if(row.startswith('0')):
        base_canal = row
        tv = base_canal[:2]
        canal = base_canal[2:][:9]
        list_time = filter(bool, re.split('[A-Z]+', row.split('X')[1]))
        if row[12:].startswith("B"):
          list_time.insert(0, 0)

        start = datetime.strptime(start_date + " " + start_time, "%Y%m%d %H:%M")

        u32start = time.mktime(start.timetuple())
        u32end = 0
        fg = False

        for item in list_time:
        #   print "item => ", str(item)
          duration = int(item) * 60
          if not fg:
            u32start = u32start + duration
          else:
            u32end = u32start + duration
            #print "start: " + str(u32start) + " end:" + str(u32end)
            yield Row(**{'ID_Household': household_id, 
                         'start_date': start_date , 
                         'end_date' : end_date, 
                         'end_time': end_time, 
                         'region': region, 
                         'individual_code' : indiviadual_code,
                         'base_canal' : base_canal,
                         'tv' : tv,
                         'canal' : canal,
                         'start_time': u32start,
                         'end_time': u32end
                     })
            u32start = u32end

          fg = not fg

spark = SparkSession(sc)
hasattr(ibope_txt, "toDF")
ibope_df = ibope_txt.flatMap(processFileLines).filter(bool).toDF()
ibope_df.write.saveAsTable('ibope_tbl_times_v1')