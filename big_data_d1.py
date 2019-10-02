# To start the pyspark shell
pyspark

rdd = sc.textFile("Complete_Shakespeare.txt")

rdd.count()
#124787

rdd_words = rdd.flatMap(lambda l: l.split(" "))

rdd_words = rdd.flatMap(lambda l: l.split())

rdd_words.count()
#904061


rdd_words.take(2)
#[u'The', u'Project']


rdd_words.count()
#1410671


rdd_words.distinct().count()
#67780


key_value_rdd = rdd_words.map(lambda x: (x,1))

key_value_rdd.take(5)
[(u'The', 1), (u'Project', 1), (u'Gutenberg', 1), (u'EBook', 1), (u'of', 1)]


word_counts_rdd = key_value_rdd.reduceByKey(lambda x,y: x+y)

word_counts_rdd.take(5)
[(u'fawn', 11), (u'considered-', 1), (u'Fame,', 3), (u'mustachio', 1), (u'protested,', 1)]

flipped_rdd = word_counts_rdd.map(lambda x: (x[1], x[0]))

flipped_rdd.take(5)
[(11, u'fawn'), (1, u'considered-'), (3, u'Fame,'), (1, u'mustachio'), (1, u'protested,')]


results_rdd = flipped_rdd.sortByKey(False)

results_rdd.take(5)
[(23407, u'the'), (19540, u'I'), (18358, u'and'), (15682, u'to'), (15649, u'of')]



### ************
