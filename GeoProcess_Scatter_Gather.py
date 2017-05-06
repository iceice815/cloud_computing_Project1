import json
from mpi4py import MPI
import numpy as np
GRID="melbGrid.json"

def get_box_dict(grid):
    d={}
    grids = json.load(open(grid, "r", encoding="utf8"))
    for box in grids["features"]:
        xmax = box['properties']["xmax"]
        xmin = box['properties']["xmin"]
        ymax = box['properties']["ymax"]
        ymin = box['properties']["ymin"]
        d.update({box['properties']["id"]:[xmax,xmin,ymax,ymin]})
    return d

def get_box(coordinate,dict):
    x = coordinate[0]
    y = coordinate[1]
    for grid in dict:
        value=dict[grid]
        if x <= value[0] and x >= value[1] and y <= value[2] and y >= value[3]:
            return grid

def mergeDict(dict1,dict2):
    newDict=dict1
    for key2 in dict2:
        if key2 in newDict.keys():
            newDict.update({key2:newDict[key2]+dict2[key2]})
        else:
            newDict.update({key2:dict2[key2]})
    return newDict

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

if rank==0:
    data=[]
    with open("bigTwitter.json","r",encoding="utf-8") as raw:
        for line in raw:
            try:
                lineJson=json.loads(line[:-2])
                data.append(lineJson["json"]["coordinates"]["coordinates"])
            except:
                pass
        data=np.array_split(data,size)
else:
    data =None
#Scatter data in core size
data = comm.scatter(data,root=0)

box,row,col={},{},{}
d=get_box_dict(GRID)

for coordinate in data:
    c=get_box(coordinate,d)
    if c is not None:
        box[c] = box.setdefault(c,0)+1
        row[c[0]]=row.setdefault(c[0],0)+1
        col[c[1]]=col.setdefault(c[1],0)+1
data_send = [box,row,col]
#Gather data for further process
newData=comm.gather(data_send,root=0)

outputList=[]
outputDict={}
#merge data and print the outputs
if rank==0:
    for lists in newData:
        for list in lists:
            outputList.append(list)
    for output in outputList:
        outputDict=mergeDict(outputDict,output)


    eles=sorted(outputDict.items(), key=lambda x: x[1], reverse=True)
    for key,value in eles:
        if len(key)==2:
            print (key,value)
    for key,value in eles:
        if len(key)==1 and not key.isdigit():
            print(key+"-Row",value)
    for key,value in eles:
        if key.isdigit():
            print("Column " + key, value)
