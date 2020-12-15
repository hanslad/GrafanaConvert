import sqlite3
from sqlite3 import Error
import json
import io

def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn

def convert(sconn, rconn):

    cur = sconn.cursor()
    cur.execute("SELECT id,data FROM dashboard")
    res = {}
    total = 0
    for row in cur:
        id = str(row[0])
        data = row[1]
        if data is None or data == "":
            continue
        # try:
        #     _data = str(data,'utf-8')
        # except Exception as e:
        #     print(data)
        #     print(e)
        #data  = str(row[1]).decode('utf-16')
        jdata =  json.loads(data)        
        with io.open("before.json", 'w', encoding='utf8') as f:
            f.write(json.dumps(jdata, indent=2, sort_keys=True, ensure_ascii = False))
        count = replace_targets(jdata,0)
        total = total + count
        with io.open("after.json", 'w', encoding='utf8') as f:
            f.write(json.dumps(jdata, indent=2, sort_keys=True, ensure_ascii = False))
        res[id] = jdata
    # for key in res:        
    #     print(res[key])
    print("Total:")    
    print(str(total))        

def replace_targets(jpc,idx):
    count = 0
    if "panels" not in jpc.keys():
        return count
    for pa in jpc["panels"]:
        if "type" in  pa.keys():
            print(pa["type"])
        if "targets" in  pa.keys():
            #targets=[]
            #     trgts = pa["targets"]           
            trgts = pa["targets"]           
            for ta in trgts:            
                count = count + 1
                if "nodeChain" in  ta.keys():
                    ta["nodeChain"] = []
                #targets.append(new_target(ta))     
                #print(ta)
            #pa["targets"] = targets
        count = count + replace_targets(pa,idx+1)
    return count

def new_target(t):
    print(t["name"])
    # with io.open("new_ds_temp.json", 'r', encoding='utf8') as f:
    #     new = json.load(f)
    return t




def main():
    sdatabase = r"grafana.db" #source
    rdatabase = r"grafana2.db" #result

    # create a database connection
    sconn = create_connection(sdatabase)
    rconn = create_connection(rdatabase)
    with sconn, rconn:
        print("Start")
        convert(sconn,rconn)



if __name__ == '__main__':
    main()