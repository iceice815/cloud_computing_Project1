import json
from mpi4py import MPI
import numpy as np
GRID="melbGrid.json"

def get_block_dict(Grid):
    d={}
    grids = json.load(open(GRID, "r", encoding="utf8"))
    for block in grids["features"]:
        xmax = block['properties']["xmax"]
        xmin = block['properties']["xmin"]
        ymax = block['properties']["ymax"]
        ymin = block['properties']["ymin"]
        d.update({block['properties']["id"]:[xmax,xmin,ymax,ymin]})
    return d

def get_block(coordinate,dict):
    x = coordinate[0]
    y = coordinate[1]
    for grid in dict:
        value=dict[grid]
        if x <= value[0] and x >= value[1] and y <= value[2] and y >= value[3]:
            return grid
def merge(dict1,dict2):
    combined_keys={key for d in [dict1,dict2] for key in d}
    set_new = {key:(dict1.setdefault(key, 0) + dict2.setdefault(key, 0)) for key in combined_keys}
    return set_new

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

data = comm.scatter(data,root=0)

blk,row,col={},{},{}
d=get_block_dict(GRID)

for coordinate in data:
    c=get_block(coordinate,d)
    if c is not None:
        blk[c] = blk.setdefault(c,0)+1
        row[c[0]]=row.setdefault(c[0],0)+1
        col[c[1]]=col.setdefault(c[1],0)+1

data_send = [blk,row,col]
newData=comm.gather(data_send,root=0)

outputList=[]
outputDict={}
if rank==0:
    for lists in newData:
        for list in lists:
            outputList.append(list)
    for output in outputList:
        outputDict=merge(outputDict,output)


#____Output Result____:
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
