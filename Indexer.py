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
	indexDir = SimpleFSDirectory(File("./indexDir"))
	writerConfig = IndexWriterConfig(Version.LUCENE_4_10_1, StandardAnalyzer())
	writer = IndexWriter(indexDir, writerConfig)
 
	print "%d docs in index" % writer.numDocs()
	print "Reading lines from sys.stdin..."
	for n, l in enumerate(sys.stdin):
		doc = Document()
		doc.add(Field("text", l, Field.Store.YES, Field.Index.ANALYZED))
		writer.addDocument(doc)
	print "Indexed %d lines from stdin (%d docs in index)" % (n, writer.numDocs())
	print "Closing index of %d docs..." % writer.numDocs()
	writer.close()
