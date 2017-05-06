import json
GRID="melbGrid.json"

def get_block(tweet):
    x=tweet["json"]["coordinates"]["coordinates"][0]
    y=tweet["json"]["coordinates"]["coordinates"][1]
    grids = json.load(open(GRID, "r", encoding="utf8"))
    for block in grids["features"]:
        xmax = block['properties']["xmax"]
        xmin = block['properties']["xmin"]
        ymax = block['properties']["ymax"]
        ymin = block['properties']["ymin"]
        if x <= xmax and x >= xmin and y <= ymax and y>=ymin :
            return block['properties']["id"]

