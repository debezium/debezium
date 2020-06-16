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
firstenrties = int(sys.argv[3])

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
xmax=firstenrties    #max(id)
ymin=0

del db[0:xmin]
del db[xmax:len(id)]
del kafka[0:xmin]
del kafka[xmax:len(id)]




x = []
y = []

for i in range(xmin,xmax):
    x.append((kafka[i]) / 1000)
    y.append(id[i])
plt.scatter(x,y,s=0.01,c='lightblue')


x = []
y = []
for i in range(xmin,xmax):
    x.append((db[i]) / 1000)
    y.append(id[i])    

axes = plt.gca()
axes.set_ylim([0,firstenrties])
plt.xlabel('millisecond')
plt.ylabel('entries ')


plt.scatter(x,y,s=0.01,c='red')




plt.savefig(Plotfilename)
