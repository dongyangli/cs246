import sys
import lucene
 
from java.io import File
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.index import IndexReader
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.store import SimpleFSDirectory
from org.apache.lucene.util import Version

from sets import Set
import MySQLdb as mdb

import Convert as cvt
import re

SIZE = 4

management_bc = [
"Wikipedia", 
"Wikidata", 
"Categories", 
"wikiproject", 
"lists", 
"mediawiki", 
"template", 
"user", 
"portal", 
"categories", 
"articles", 
"pages" , 
"stub",
"wikidata",
"wikipedia"]

def predict(user_query, analyzer, reader, searcher, test = "individual"):
    if test == "individual":
        SIZE = 20;

    """ lucene indexing """
    query = QueryParser(Version.LUCENE_4_10_1, "title", analyzer).parse(user_query)
    
    MAX = 10
    hits = searcher.search(query, MAX)

    page_ids = {}
    print "Found %d document(s) that matched query '%s':" % (hits.totalHits, query)
    for hit in hits.scoreDocs:
        #print hit.score, hit.doc, hit.toString()
        doc = searcher.doc(hit.doc)
        page_id = doc.get("id")
        page_ids[page_id] = hit.score
        #print page_id

        if test == "individual":
            page_title = doc.get("title")
            print page_id, page_title
        
        
    if not page_ids:
        print "empty page_ids!!!"

    """ look up base categories """
    base_categories = {} 
    try:
        con = mdb.connect('localhost', 'root', '', 'cs246')
        cur = con.cursor()
        for page_id in page_ids:
            cur.execute("SELECT cl_to FROM article_to_x WHERE cl_from = %s", [page_id])
            rows = cur.fetchall()
            for row in rows:
                title = row[0]
                """ 
                remove instead all those nodes whose labels contain any of the following strings: 
                Wikipedia, wikiproject, lists, mediawiki, template, user, portal, categories, 
                articles, pages and stub. 
                """
                valid = True
                for mbc in management_bc:
                    if mbc in title:
                        valid = False
                        break
                if not valid:
                    continue
                """ make a selection on the base category here """
                # keep 25 Base Categories, according to the sum of lucene score
                if base_categories.has_key(title):
                    """ simply summed up for now """
                    base_categories[title] += page_ids[page_id]
                else:
                    base_categories[title] = page_ids[page_id]
                #print row[0]
        if not base_categories:
            print "empty base categories!!!"
        sorted_bc = sorted(base_categories.items(), key=lambda x:x[1], reverse=True)
        top25base_categories = dict()
        """ make sure every base category contains at least one key word!!!! """

        for i, bc in enumerate(sorted_bc):
            if i < 25:
                #print bc[0]
                top25base_categories[bc[0]] = bc[1]
        #for bc in top25base_categories:    
            #print(bc)

        """ look up goal categories """
        """ select some goal categories based one the based categories selected above """
        goal_categories = {}
        for bc in top25base_categories:
            if test == "individual":
                print bc
            score = top25base_categories[bc]
            cur.execute("SELECT cl_to, distance FROM dist WHERE cl_from = %s", [bc])
            rows = cur.fetchall() # return 99 goal categories
            for row in rows: # 99?
                # store the score for each goal category
                gc = row[0]
                dist = row[1]
                if goal_categories.has_key(gc):
                    goal_categories[gc] += score/(dist*dist + 0.0001)
                else:
                    goal_categories[gc] = score/(dist*dist + 0.0001)
                #print row
        if not goal_categories:
            print "empty goal category set!!!"
        """ Use the score of goal categories, find top 3 goal category, return them """
        sorted_gc = sorted(goal_categories.items(), key=lambda x:x[1], reverse=True)
        #for gc in sorted_gc:
            #print gc
        gcs = sorted_gc[0:SIZE]
        return gcs


    except mdb.Error, e:
        print "Error %d: %s" % (e.args[0],e.args[1])
        sys.exit(1)
    finally:
        if con:    
            con.close()





def group_tests():

    TP = 0.0
    FN = 0.0
    n = 0.0
    precision = 0
    recall = 0

    lucene.initVM()
    analyzer = StandardAnalyzer(Version.LUCENE_4_10_1)
    reader = IndexReader.open(SimpleFSDirectory(File("./articleTitleIndex/")))
    searcher = IndexSearcher(reader)
    with open('Labeled800Queries/labeler3.txt', 'r') as f:
        for line in f:
            n += 1
            line = line.split('\t')
            user_query = line[0]
            labels = line[1:]
            user_query = re.sub('[^0-9a-zA-Z]+', ' ', user_query)
            print user_query
            print labels
            res =  predict(user_query, analyzer, reader, searcher, test = "group")

            converted_res = []
            for label in res:
                #print label[0]
                converted_res.append(cvt.WikiToKDD[label[0].replace('_', ' ')])

            if not res:
                print "empty goal category set"
            print converted_res

            """ compare labels and converted_res """
            for label in labels:
                label = label.replace('\r', '')
                label = label.replace('\n', '')
                if label not in cvt.WikiToKDD.values():
                    continue
                #print label
                if label in converted_res:
                    TP += 1.0
                else:
                    FN += 1.0
            
            print "=========================================================="

    precision = TP/(SIZE*n)
    recall = TP/(TP+FN)

    print "precision:", precision
    print "recall:", recall



"""
base category:25
page_ids: 100
goal_categories: 4

label1: 
precision: 0.1603125
recall: 0.184998196899

label2:
precision: 0.0990625
recall: 0.180626780627

label3:
precision: 0.1434375
recall: 0.158988569449

3, 1000 page_ids, 100 base_categories, 4 goal_categories
precision: 0.1675
recall: 0.18565985452
"""

def individual_test():

    user_query = "microsoft forms"
    lucene.initVM()
    analyzer = StandardAnalyzer(Version.LUCENE_4_10_1)
    reader = IndexReader.open(SimpleFSDirectory(File("./articleTitleIndex_withTitle/")))
    searcher = IndexSearcher(reader)
    res =  predict(user_query, analyzer, reader, searcher)
    print "goal_categories:"
    print res
    converted_res = []
    for label in res:
        #print label[0]
        converted_res.append(cvt.WikiToKDD[label[0].replace('_', ' ')])
    if not res:
        print "empty goal category set"
    print "converted goal_categories:"
    print converted_res


if __name__ == "__main__":
    individual_test()

