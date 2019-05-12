import pandas as pd
import numpy as np
import requests
import os
import json

from tqdm import tqdm
from SPARQLWrapper import SPARQLWrapper, JSON

cl = pd.read_csv("closure.csv", index_col=0)
#sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
sparql = SPARQLWrapper("http://sitaware.isi.edu:8080/bigdata/namespace/wdq/sparql")


class Wikifier():
    def __init__(self, items, path, top_n=10):
        self.items = items
        self.path = path
        self.top_n = top_n

    def to_csv(self, df, name):
        file_path = os.path.join(self.path, name)
        df.to_csv(file_path)

    def get_instances(self, qids):
        """
        Gets instance of proprety of all qnodes in list. Returns dict of qnode:instance_of
        """
        qids = " ".join(["(wd:{})".format(q) for q in qids])
        sparql.setQuery("select distinct ?item ?class where {{ VALUES (?item) {{ {} }} ?item wdt:P31 ?class .}}".format(qids))
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        
        instances = {}
        for result in results["results"]["bindings"]:
            qid = result['item']['value'].split("/")[-1]
            cls = result['class']['value'].split("/")[-1]
            if qid in instances:
                instances[qid].append(cls)
            else:
                instances[qid] = [cls]
        return instances

    def get_qnodes(self, search_term):
        """
        Returns list of all matching qnodes for a given search term
        """
        url = "https://www.wikidata.org/w/api.php"
        params = {
            "action":"wbsearchentities",
            "format":"json",
            "language":"en",
            "limit":"max"
        }
        params["search"] = search_term
        r = requests.get(url, params=params)
        if r.ok:
            res = r.json()
            qnodes = [x['id'] for x in res.get("search",[])]
        
        return qnodes

    def get_all_qnodes(self):
        """
        Gets qnodes of all items
        """
        self.qnodes = {}
        for item in tqdm(self.items):
            self.qnodes[item] = self.get_qnodes(item)
        return self.qnodes

    def get_wiki_df(self):
        """
        Construct the wikified df
        """
        self.wiki = pd.DataFrame()
        for item in tqdm(self.items):
            instances = self.get_instances(self.qnodes[item])
            for q in self.qnodes[item]:
                s = pd.Series()
                s["items"] = item
                s.name = q
                if q not in instances:
                    continue
                related = set()
                for i in instances[q]:
                    related.add(i)
                    related.update(cl[cl.qid_id == i].clid.tolist())
                for r in related:
                    s[r] = True
                self.wiki = self.wiki.append(s)
        self.wiki.index.name = "qnode"
        self.wiki = self.wiki.reset_index()
        self.wiki = self.wiki.set_index(['items','qnode'])
        self.wiki = self.wiki.fillna(0)
        return self.wiki

    def get_histogram(self):
        """
        Caluclate subtotals and histogram
        """
        self.subtotals = self.wiki.groupby('items').sum()
        self.his = pd.DataFrame()
        cols=["0","1","2","3",">4"]
        for i in range(len(self.subtotals.columns)):
            counts, bins = np.histogram(self.subtotals.iloc[:, i], bins = [0,1,2,3, 4, float('inf')])
            self.his = self.his.append(pd.Series(counts,name=self.wiki.columns[i], index=cols))
        self.his = self.his.sort_values(['1'],ascending=False)
        self.his = self.his/len(self.subtotals)
        qids = self.his.index.tolist()
        names = self.get_names(qids)
        names = [names[q] for q in qids]
        self.his["name"] = names
        return self.his
    
    def get_name(self, qids):
        """
        Get labels of list of qnodes (max 50)
        """
        url = "https://www.wikidata.org/w/api.php"
        params = {
            "action":"wbgetentities",
            "format":"json",
            "props":"labels",
            "languages":"en",
            "limit":"max"
        }
        params["ids"] = "|".join(qids)
        r = requests.get(url, params=params)
        name = {}
        if r.ok:
            res = r.json()
            for qid in qids:
                name[qid] = res["entities"][qid]["labels"].get("en",{}).get("value","")
        
        return name
    
    def get_names(self, qids):
        """
        Get labels for lists > 50 by sending requests in batches.
        """
        names = {}
        last = 0
        print("Retreiving names")
        for i in tqdm(range(50, len(qids), 50)):
            names.update(self.get_name(qids[i-50:i]))
            last = i
        names.update(self.get_name(qids[last:len(qids)]))
        return names

    def get_result(self):
        """
        Get the final results.
        """
        self.result = self.his[['1','name']][:self.top_n]
        self.result = pd.DataFrame(self.result)
        self.result = self.result.rename({'1':'confidence'}, axis='columns')
        self.result['confidence'] *= 100
        self.result.index.name = "class"
        return self.result
    
    def build_wiki_json(self):
        idx = self.wiki.index.values.tolist()
        self.wiki_map = {k:[] for k in self.items}
        for k,v in idx:
            self.wiki_map[k].append(v)
        return self.wiki_map

    def wikify(self):
        """
        Main function to run everything together.
        """
        print("Retrieving all qnodes")
        self.get_all_qnodes()
        print("Building wikified data")
        self.get_wiki_df()
        self.to_csv(self.wiki, "wikified.csv")
        print("Calculating histogram")
        self.get_histogram()
        self.to_csv(self.subtotals, "subtotals.csv")
        self.to_csv(self.his, "histogram.csv")
        print("Result")
        print(self.get_result())
        self.to_csv(self.result, "candidates.csv")
        print("Wikimap")
        self.build_wiki_json()
        json.dump(self.wiki_map, open(os.path.join(self.path, "wiki_map.json"), 'w+'))


