# http://graus.nu/blog/pylucene-4-0-in-60-seconds-tutorial/
# http://www.tutorialspoint.com/lucene/lucene_overview.htm

import MySQLdb as mdb
import sys
import lucene
 
from java.io import File
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field
from org.apache.lucene.index import IndexWriter, IndexWriterConfig
from org.apache.lucene.store import SimpleFSDirectory
from org.apache.lucene.util import Version

if __name__ == "__main__":
	
	lucene.initVM()
	indexDir = SimpleFSDirectory(File("./articleTitleIndex_withTitle"))
	writerConfig = IndexWriterConfig(Version.LUCENE_4_10_1, StandardAnalyzer())
	writer = IndexWriter(indexDir, writerConfig)

	try:
		con = mdb.connect('localhost', 'root', '', 'cs246')
		cur = con.cursor()
		cur.execute("SELECT * FROM article_page;")
		rows = cur.fetchall()
		n = 0
		for row in rows:
			n = n+1
			page_id = str(row[0])
			page_title = str(row[1]).replace('_', ' ')

			doc = Document()
			doc.add(Field("title", page_title, Field.Store.YES, Field.Index.ANALYZED_NO_NORMS))
			doc.add(Field("id", page_id, Field.Store.YES, Field.Index.NO))
			writer.addDocument(doc)
		print "total number of tuples", n
	except mdb.Error, e:
		print "Error %d: %s" % (e.args[0],e.args[1])
		sys.exit(1)
	finally:
		if con:    
			con.close()

	print "Created (%d docs in index)" % (writer.numDocs())
	print "Closing index of %d docs..." % writer.numDocs()
	writer.close()

