postclick = '_PostClick'
postimp = '_PostImpression'

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
autodicplace = 'AUTO DICTIONARY PLACEMENT'
autodicord = 'AUTO DICTIONARY ORDER'
apifile = 'API_FILE'
apifields = 'API_FIELDS'
apimerge = 'API_MERGE'
transform = 'TRANSFORM'
header = 'HEADER'
omit_plan = 'OMIT_PLAN'
date = 'Date'
impressions = 'Impressions'
clicks = 'Clicks'
cost = 'Net Cost'
views = 'Video Views'
views25 = 'Video Views 25'
views50 = 'Video Views 50'
views75 = 'Video Views 75'
views100 = 'Video Views 100'
landingpage = 'Landing Page'
homepage = 'Homepage'
btnclick = 'Button Click'
purchase = 'Purchase'
signup = 'Sign Up'
gplay = 'Game Played'
gplay3 = 'Games Played 3'
gplay6 = 'Games Played 6'
landingpagepi = 'Landing Page' + postimp
homepagepi = 'Homepage' + postimp
btnclickpi = 'Button Click' + postimp
purchasepi = 'Purchase' + postimp
signuppi = 'Sign Up' + postimp
gplaypi = 'Game Played' + postimp
gplay3pi = 'Games Played 3' + postimp
gplay6pi = 'Games Played 6' + postimp
landingpagepc = 'Landing Page' + postclick
homepagepc = 'Homepage' + postclick
btnclickpc = 'Button Click' + postclick
purchasepc = 'Purchase' + postclick
signuppc = 'Sign Up' + postclick
gplaypc = 'Game Played' + postclick
gplay3pc = 'Games Played 3' + postclick
gplay6pc = 'Games Played 6' + postclick
conv1 = 'Conv1_CPA'
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
          filenameerror, startdate, enddate, dropcol, autodicplace, autodicord,
          apifile, apifields, apimerge, transform, header, omit_plan]

datacol = [date, impressions, clicks, cost, views, views25, views50, views75,
           views100, landingpage, homepage, btnclick, purchase, signup,
           gplay, gplay3, gplay6, landingpagepi, homepagepi, btnclickpi,
           purchasepi, signuppi, gplaypi, gplay3pi, gplay6pi, landingpagepc,
           homepagepc, btnclickpc, purchasepc, signuppc, gplaypc, gplay3pc,
           gplay6pc, conv1, conv2, conv3, conv4, conv5, conv6, conv7, conv8,
           conv9, conv10]

vmkeys += datacol
barsplitcol = ([fullplacename, dropcol, autodicord, apifields] + datacol)

datecol = [startdate, enddate]
datadatecol = [date]
datafloatcol = datacol[:]
datafloatcol.remove(date)

pathraw = 'Raw Data/'

AD_COST = 'Adserving Cost'
AM_CPM = 'CPM'
