import datetime

starttime = datetime.datetime.now()
print(starttime)
print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
endtime = datetime.datetime.now()
print('cost seconds :'+ str((starttime-endtime).seconds))
print( ' succeeded at '+ datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
