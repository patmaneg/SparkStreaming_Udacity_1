import json
import time

fichero='/home/workspace/data/uber.json'
with open(fichero) as f:
    i = 0
    #data = json.load(f)
    for line in f:
        print("line:",line)
        message = json.dumps(line).encode('utf-8')
        print("message:",message)
        if i > 10:
            break
        i = i +1
    
    
