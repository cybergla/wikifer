import pandas as pd
from collections import deque

from SPARQLWrapper import SPARQLWrapper, JSON
from tqdm import tqdm
from peewee import *

df = pd.read_csv("wd-classes-min-5-instances.csv")
sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
db = SqliteDatabase("graph.db")

# DB Models
class BaseModel(Model):
    class Meta:
        database = db
    
class Qnode(BaseModel):
    qid = TextField(primary_key=True)
    url = TextField(unique=True)
    instances = IntegerField()

class Closure(BaseModel):
    qid = ForeignKeyField(Qnode, backref='qnodes')
    clid = TextField()

    class Meta:
        constraints = [SQL('UNIQUE (qid_id, clid)')]
    

db.create_tables([Qnode, Closure])

def add_to_qnodes():
    df = pd.read_csv("wd-classes-min-5-instances.csv")
    for i in range(len(df)):
        row = df.iloc[i]
        Qnode.create(qid=row.id, url=row.s, instances=row.c)


def get_transitive_closure(qid):
    sparql.setQuery("select DISTINCT ?s where {{ wd:{} wdt:P31/wdt:P279* ?s .  }}".format(qid))
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()

    closure = []
    for result in results["results"]["bindings"]:
        closure.append(result['s']['value'].split("/")[-1])

    if len(closure) == 0:
        sparql.setQuery("select DISTINCT ?s where {{ wd:{} wdt:P279* ?s .  }}".format(qid))
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        for result in results["results"]["bindings"]:
            closure.append(result['s']['value'].split("/")[-1])

    return closure

# print(get_transitive_closure("Q81163"))

def add_current_data():
    df = pd.read_csv("wd_classes_with_closure_2.csv",header=None)
    print("Adding data")
    for i in tqdm(range(len(df))):
        row = df.iloc[i]
        Closure.create(qid=row[0], clid=row[1])

def get_all_closures():
    query = (Qnode
            .select(Qnode.qid)
            .join(Closure, on=(Qnode.qid==Closure.qid), join_type=JOIN.LEFT_OUTER)
            .where(Closure.qid_id == None))
    
    if not query.exists():
        print("No more qnodes found")
        return
    
    for id in query:
        cl = get_transitive_closure(id)
        print(id)
        for clid in cl:
            Closure.create(qid=id, clid=clid)
        

if __name__ == "__main__":
    get_all_closures()
