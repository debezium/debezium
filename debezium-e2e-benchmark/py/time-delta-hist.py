import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.pyplot as plt2
import numpy as np
import csv
import sys
from scipy import *

csvfile = sys.argv[1]
Plotfilename = sys.argv[2]

x = []
y = []
db = []
kafka = []
id = []




with open(csvfile) as csvfile:
    tpcdata = csv.reader(csvfile, delimiter=';')
    inittmp = 0
    for row in tpcdata:
        if inittmp == 0:
            inittmp = int(row[1])
        db.append(int(row[1]) - inittmp)
        kafka.append(int(row[0]) - inittmp)
        id.append(int(row[3]))


        
xmin=0
xmax=len(id)
#xmin=19000    #0
##xmax=9000    #max(id)
ymin=0

del db[0:xmin]
del db[xmax:len(id)]
del kafka[0:xmin]
del kafka[xmax:len(id)]


x = []
y = []

#for idx, e in enumerate(db):
for i in range(xmin,xmax):
    x.append((kafka[i]-db[i]) / 1000)
    #x.append((kafka2[i]) / 1000)
    y.append(id[i])


num_bins = 100
n, bins, patches = plt.hist(x, num_bins, facecolor='blue', alpha=0.5)

plt.xlabel('milisecond')




plt.savefig(Plotfilename)


