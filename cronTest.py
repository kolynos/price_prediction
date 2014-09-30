'''
Created on 30.09.2014

@author: colinos
'''
import time

log_file='log_'+time.strftime("%d_%m_%Y_%H_%M_%S")+'.csv'

print log_file

f=open(log_file,'w')

f.write('test')

f.close()