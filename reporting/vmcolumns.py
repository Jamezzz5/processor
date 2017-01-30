
vendorkey = 'Vendor Key'
filename = 'FILENAME'
firstrow = 'FIRSTROW'
lastrow = 'LASTROW'
fullplacename = 'Full Placement Name'
placement = 'Placement Name'
filenamedict = 'FILENAME_DICTIONARY'
filenameerror = 'FILENAME_ERROR'
startdate = 'START DATE'
enddate = 'END DATE'
dropcol = 'DROP_COLUMNS'
autodicord = 'AUTO DICTIONARY ORDER'
apifile = 'API_FILE'
apifields = 'API_FIELDS'
apimerge = 'API_MERGE'
date = 'Date'
impressions = 'Impressions'
clicks = 'Clicks'
cost = 'Net Cost'
views = 'Video Views'
views25 = 'Video Views 25%'
views50 = 'Video Views 50%'
views75 = 'Video Views 75%'
views100 = 'Video Views 100%'
conv1 = 'Conv1 - CPA'
conv2 = 'Conv2'
conv3 = 'Conv3'
conv4 = 'Conv4'
conv5 = 'Conv5'
conv6 = 'Conv6'
conv7 = 'Conv7'
conv8 = 'Conv8'
conv9 = 'Conv9'
conv10 = 'Conv10'

vmkeys = [filename, firstrow, lastrow, fullplacename, placement, filenamedict,
          filenameerror, startdate, enddate, dropcol, autodicord, apifile,
          apifields, apimerge]

datacol = [date, impressions, clicks, cost, views, views25, views50, views75,
           views100, conv1, conv2, conv3, conv4, conv5, conv6, conv7, conv8,
           conv9, conv10]

vmkeys = vmkeys + datacol
barsplitcol = ([fullplacename, dropcol, autodicord, apifields] + datacol)

datecol = [startdate, enddate]
datadatecol = [date]
datafloatcol = [impressions, clicks, cost, views, views25, views50, views75,
                views100, conv1, conv2, conv3, conv4, conv5, conv6, conv7,
                conv8, conv9, conv10]


pathraw = 'Raw Data/'
