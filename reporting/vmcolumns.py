
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
nullimps = 'NULL_IMPRESSIONS'
nullclicks = 'NULL_CLICKS'
nullcost = 'NULL_COST'
nullviews = 'NULL_VIEWS'
nullviews25 = 'NULL_VIEWS25'
nullviews50 = 'NULL_VIEWS50'
nullviews75 = 'NULL_VIEWS75'
nullviews100 = 'NULL_VIEWS100'
nullconv1 = 'NULL_CONV'
nullconv2 = 'NULL_CONV2'
nullconv3 = 'NULL_CONV3'
nullconv4 = 'NULL_CONV4'
nullconv5 = 'NULL_CONV5'
nullconv6 = 'NULL_CONV6'
nullconv7 = 'NULL_CONV7'
nullconv8 = 'NULL_CONV8'
nullconv9 = 'NULL_CONV9'
nullconv10 = 'NULL_CONV10'
nullimpssd = 'NULL IMPS - SD'
nullimpsed = 'NULL IMPS - ED'
nullclicksd = 'NULL CLICK - SD'
nullclicked = 'NULL CLICK - ED'
nullcostsd = 'NULL COST - SD'
nullcosted = 'NULL COST - ED'
nullviewssd = 'NULL VIEWS - SD'
nullviewsed = 'NULL VIEWS - ED'
nullviews25sd = 'NULL VIEWS25 - SD'
nullviews25ed = 'NULL VIEWS25 - ED'
nullviews50sd = 'NULL VIEWS50 - SD'
nullviews50ed = 'NULL VIEWS50 - ED'
nullviews75sd = 'NULL VIEWS75 - SD'
nullviews75ed = 'NULL VIEWS75 - ED'
nullviews100sd = 'NULL VIEWS100 - SD'
nullviews100ed = 'NULL VIEWS100 - ED'
nullconv1sd = 'NULL CONV1 - SD'
nullconv1ed = 'NULL CONV1 - ED'
nullconv2sd = 'NULL CONV2 - SD'
nullconv2ed = 'NULL CONV2 - ED'
nullconv3sd = 'NULL CONV3 - SD'
nullconv3ed = 'NULL CONV3 - ED'
nullconv4sd = 'NULL CONV4 - SD'
nullconv4ed = 'NULL CONV4 - ED'
nullconv5sd = 'NULL CONV5 - SD'
nullconv5ed = 'NULL CONV5 - ED'
nullconv6sd = 'NULL CONV6 - SD'
nullconv6ed = 'NULL CONV6 - ED'
nullconv7sd = 'NULL CONV7 - SD'
nullconv7ed = 'NULL CONV7 - ED'
nullconv8sd = 'NULL CONV8 - SD'
nullconv8ed = 'NULL CONV8 - ED'
nullconv9sd = 'NULL CONV9 - SD'
nullconv9ed = 'NULL CONV9 - ED'
nullconv10sd = 'NULL CONV10 - SD'
nullconv10ed = 'NULL CONV10 - ED'

vmkeys = [filename, firstrow, lastrow, fullplacename, placement, filenamedict,
          filenameerror, startdate, enddate, dropcol, autodicord, apifile,
          apifields, apimerge]

datacol = [date, impressions, clicks, cost, views, views25, views50, views75,
           views100, conv1, conv2, conv3, conv4, conv5, conv6, conv7, conv8,
           conv9, conv10]

nullcol = [nullimps, nullclicks, nullcost, nullviews, nullviews25, nullviews50,
           nullviews75, nullviews100, nullconv1, nullconv2, nullconv3,
           nullconv4, nullconv5, nullconv6, nullconv7, nullconv8, nullconv9,
           nullconv10]

nulldate = [nullimpssd, nullimpsed, nullclicksd, nullclicked, nullcostsd,
            nullcosted, nullviewssd, nullviewsed, nullviews25sd, nullviews25ed,
            nullviews50sd, nullviews50ed, nullviews75sd, nullviews75ed,
            nullviews100sd, nullviews100ed, nullconv1sd, nullconv1ed,
            nullconv2sd, nullconv2ed, nullconv3sd, nullconv3ed, nullconv4sd,
            nullconv4ed, nullconv5sd, nullconv5ed, nullconv6sd, nullconv6ed,
            nullconv7sd, nullconv7ed, nullconv8sd, nullconv8ed, nullconv9sd,
            nullconv9ed, nullconv10sd, nullconv10ed]

vmkeys = vmkeys + datacol + nullcol + nulldate
barsplitcol = ([fullplacename, dropcol, autodicord, apifields] + nullcol +
               nulldate)

datecol = [startdate, enddate] + nulldate
datadatecol = [date] + nulldate
datafloatcol = [impressions, clicks, cost, views, views25, views50, views75,
                views100, conv1, conv2, conv3, conv4, conv5, conv6, conv7,
                conv8, conv9, conv10]

nullcoldic = dict(zip(datafloatcol, nullcol))
nulldatedic = dict(zip(datafloatcol, zip(nulldate, nulldate[1:])[::2]))

pathraw = 'Raw Data/'
