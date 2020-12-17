import sqlite3
from sqlite3 import Error
import json
import io

NS_URI = "http://prediktor.no/apis/semantics/AkershusEnergi"

TA_VER = []

def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn

def convert(conn):

    cur = conn.cursor()
    cur.execute("SELECT id,data FROM dashboard")
    #cur.execute("SELECT id,data FROM dashboard where title = 'HPL'")
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
        # with io.open("before.json", 'w', encoding='utf8') as f:
        #     f.write(json.dumps(jdata, indent=2, sort_keys=True, ensure_ascii = False))
        count = replace_targets(jdata,0)
        total = total + count
        res[id] = json.dumps( jdata)
    # for key in res:        
    #    print(res[key])
    # with io.open("after.json", 'w', encoding='utf8') as f:
    #     f.write(json.dumps(res, indent=2, sort_keys=True, ensure_ascii = False))
    with io.open("ta_versions.json", 'w', encoding='utf8') as f:
        f.write(json.dumps(TA_VER, indent=2, sort_keys=True, ensure_ascii = False))
    print("Total:")    
    print(str(total)) 
    return res


def persist(conn, res):
    for id in res:
        data = res[id]
        sql = ''' UPDATE dashboard
              SET data = ? 
              WHERE id = ?'''
        cur = conn.cursor()
        cur.execute(sql, (data,id))
        conn.commit()    

def replace_targets(jpc,idx):
    count = 0
    if "panels" not in jpc:
        return count
    # if idx >= 1:
    #     print(jpc)
    pnls = jpc["panels"]
    for pa in pnls:
        if "targets" in pa :#and (idx > 0 or (idx == 0 and "pluginVersion" in pa)) :
            new_trgts=[]                   
            trgts = pa["targets"]           
            for ta in trgts:
                if "type" in ta.keys():
                    if ta["type"] != "timeserie":
                        print(ta["type"])            
                count = count + 1
                if "nodeChain" in  ta.keys():
                    ta["nodeChain"] = []
                if "target" in ta:
                    if ta["target"] is None:
                        #print('Panel: {pan} has empty target, uses old target def: {tar}'.format(pan = pa["title"],tar = ta ))
                        nt = ta
                    else:
                        check_ver(ta)
                        nt =  new_target(ta)
                else:
                    print('ERROR: No target in target: {}'.format(ta))
                    nt = ta
                #if(ta["type"] != "timeserie"):
                    #print('Type: {}'.format(ta["type"]))
                new_trgts.append(nt)
            with io.open("old_targets.json", 'w', encoding='utf8') as f:
                f.write(json.dumps(pa["targets"], indent=2, sort_keys=True, ensure_ascii = False)) 
            with io.open("new_targets.json", 'w', encoding='utf8') as f:
                f.write(json.dumps(new_trgts, indent=2, sort_keys=True, ensure_ascii = False))     
            pa["targets"] = new_trgts
            with io.open("pa.json", 'w', encoding='utf8') as f:
                f.write(json.dumps(pa, indent=2, sort_keys=True, ensure_ascii = False))           
        count = count + replace_targets(pa,idx+1)
    return count



def check_ver(t):
    found = False
    if not TA_VER:
        TA_VER.append(t)
        return
    for ta in TA_VER:
        all_similar = True 
        for key in ta.keys():
            if key not in t.keys():
                all_similar = False
        if all_similar:            
            for key in t.keys():
                if key not in ta.keys():
                    all_similar = False
        if all_similar:
            found = True
            break
    
    if not found:
        TA_VER.append(t)



def new_target(t):    
    with io.open("new_ds_temp.json", 'r', encoding='utf8') as f:
        nt = json.load(f)
    if "name" not in t or t["name"] is None:
        print(t)
    if "refId" in t and t["refId"] is not None:
        nt["refId"] = t["refId"]
    else:
        del nt['refId']
    if "aggregate" in t and t["aggregate"] is not None and t["aggregate"].lower() != "raw":
        nt["aggregate"] = create_Aggr( t["aggregate"])


    # else:
    #     print('No aggreagate: {}'.format(t["name"]))


    #print(refId)
    #print(t["name"])

    bname = t["browseName"].split("\\")[1]
    tar = t["target"]
    if "identifier" in tar:
        uri = tar["namespaceURI"]
        idx = tar["namespaceIndex"]
        ident = tar["identifier"]

        with io.open("new_ds_temp.json", 'r', encoding='utf8') as f:
            nt = json.load(f)
        nt["nodePath"]["node"]["nodeId"] = create_nodeid(uri,idx,ident)
        nt["nodePath"]["node"]["browseName"] = create_browseName(uri,bname)
        nt["nodePath"]["node"]["displayName"] = bname
        return nt
    else:
       print(tar)


def create_nodeid(uri, idx, ident):
    kind = "i" if isinstance(ident, int) else "s"
    return '{{"namespaceUrl":"{uri}","id":"ns={idx};{kind}={ident}"}}'.format(kind= kind, uri = uri,idx = idx, ident= str(ident))  

def create_browseName(uri, name):
    bn = {}
    bn["namespaceUrl"] = uri
    bn["name"] = name
    return bn

def create_Aggr( name):
  
    aggr = {}
    if name.lower() == "interpolative":
        aggr["nodeId"] = '{"namespaceUrl":"http://opcfoundation.org/UA/","id":"i=2341"}'
        aggr["name"] = "Interpolative"
        return aggr
    elif name.lower() == "average":
        aggr["nodeId"] = '{"namespaceUrl":"http://opcfoundation.org/UA/","id":"i=2342"}'
        aggr["name"] = "Average"
        return aggr
    elif name.lower() == "total":
        aggr["nodeId"] = '{"namespaceUrl":"http://opcfoundation.org/UA/","id":"i=2344"}'
        aggr["name"] = "Total"
        return aggr
    elif name.lower() == "count":
        aggr["nodeId"] = '{"namespaceUrl":"http://opcfoundation.org/UA/","id":"i=2352"}'
        aggr["name"] = "Count"
        return aggr
    elif name.lower() == "start":
        aggr["nodeId"] = '{"namespaceUrl":"http://opcfoundation.org/UA/","id":"i=2357"}'
        aggr["name"] = "Start"
        return aggr
    elif name.lower() == "end":
        aggr["nodeId"] = '{"namespaceUrl":"http://opcfoundation.org/UA/","id":"i=2358"}'
        aggr["name"] = "End"
        return aggr
    else:
         print("Shait")   

def main():
    sdatabase = r"..\\..\\files\\grafana.db" #source
    rdatabase = r"..\\..\\files\\grafana2.db" #result

    # create a database connection
    sconn = create_connection(sdatabase)
    rconn = create_connection(rdatabase)
    with sconn, rconn:
        print("Start")
        res = convert(sconn)
        persist(rconn,res)



if __name__ == '__main__':
    main()