from pyspark.sql.types import *
from pyspark.sql import Row
import re

ibope_txt=sc.textFile("/tmp_v2/*.MET")

def processFileLines(row):
    global household_id, start_date, end_date, start_time, end_time, region, city, sel, pay_tv, hh_kids, household_weight, indiviadual_code, start_individual, insert_index,i_region, i_city, i_sel, i_pay_tv, gender, years, housewife, head_of_household, individual_weight, canal, i_row, base_canal, tc, list_time, a_idx, b_idx, a,b, start_number, checked
    rowData = row.split('#')
    for idx, val in enumerate(rowData):
        if(rowData[idx].startswith('I')):
            start_number = ''
            checked = 0
            household_id = rowData[idx].strip()[:9]
            start_date = rowData[idx].strip()[9:][:-16]
            start_time = ':'.join(rowData[idx].strip()[17:][:-12][i:i+2] for i in range(0, len(rowData[idx].strip()[17:][:-12]), 2))
            end_date = rowData[idx].strip()[21:][:-4]
            end_time = ':'.join(rowData[idx].strip()[29:][i:i+2] for i in range(0, len(rowData[idx].strip()[29:]), 2))
       
            
        if((rowData[idx].startswith('D') and len(rowData[idx]) == 11)):
            region_data = rowData[idx].split(',')
            region = region_data[1]
            city = region_data[2]
            sel = region_data[3]
            pay_tv = region_data[4]
            hh_kids = region_data[5]
            total_char = len(rowData[idx])
            
        if((rowData[idx].startswith('W'))):
            household_weight = rowData[idx].strip()[1:]  
            #if(start_number == ''):
            start_number = start_number + rowData[idx]
            
        if(rowData[idx].startswith('Z')):
            indiviadual_code = rowData[idx]
            
        if((rowData[idx].startswith('D') and len(rowData[idx]) != 11)):
            region_data = rowData[idx].split(',')
            i_row = rowData[idx]
            i_region = region_data[1] if len(region_data) >= 2 else ''
            i_city = region_data[2] if len(region_data) >= 3 else ''
            i_sel = region_data[3] if len(region_data) >= 4 else ''
            i_pay_tv = region_data[4] if len(region_data) >= 5 else ''
            gender = region_data[5] if len(region_data) >= 6 else ''
            years = region_data[6] if len(region_data) >= 7 else ''
            housewife = region_data[7] if len(region_data) >= 8 else ''
            head_of_household = region_data[8] if len(region_data) >= 9 else ''
            total_char = len(rowData[idx])
            
        if((rowData[idx].startswith('W')) and idx > 3):
            checked = 1
            individual_weight = rowData[idx].strip()[1:]
            start_number = start_number + rowData[idx]
            
            yield Row(**{'ID_Household': household_id, 
                     'start_date': start_date , 
                     'start_time' : start_time, 
                     'end_date' : end_date, 
                     'end_time': end_time, 
                     'region': region, 
                     'city' : city,
                     'sel' : sel,
                     'pay_tv' : pay_tv,
                     'hh_kids' : hh_kids,
                     'household_weight' : filter(bool, re.split('[A-Z]+', start_number))[0],
                     'indiviadual_code' : indiviadual_code,
                     'i_region' : i_region,
                     'i_city' : i_city,
                     'i_sel' : i_sel,
                     'i_pay_tv' : i_pay_tv,
                     'gender' : gender,
                     'years' : years,
                     'housewife' : housewife,
                     'head_of_household' : head_of_household,
                     'individual_weight' : filter(bool, re.split('[A-Z]+', start_number))[-1]
                 })
            
       
        

ibope_df = ibope_txt.flatMap(processFileLines).filter(bool).toDF()
ibope_df.write.saveAsTable('ibope_tbl_individuals')