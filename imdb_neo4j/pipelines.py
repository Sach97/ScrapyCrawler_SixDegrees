# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from py2neo import Graph
from py2neo import Node
from py2neo import Relationship
from py2neo import authenticate
from py2neo import watch

# set up authentication parameters
authenticate("localhost:7474", "neo4j", "Neo4j")

class ImdbPersonPagePipeline(object):
	graph = Graph("http://localhost:7474/db/data/")

	def process_item(self, item, spider):
		print('Putting Person in Neo4J: ' + item['person_id'])
		person_node = Node("Person", id=item['person_id'],name=item['person']) #persone_node = 
		#person_node.properties['name'] = item['person']
		#self.graph.push(person_node)
		#watch("httpstream")
		#person_node.push(person_node)

		for film in item['films']:
			film_node = Node("Film", id=film,name=item['films'][film])
			#film_node.properties['name'] = item['films'][film]
			#self.graph.push(film_node)
			#film_node.push(film_node)
			person_node_acted_in_film_node = Relationship(person_node,"ACTED_IN",film_node)
			self.graph.create(person_node_acted_in_film_node)
		return item
	# def process_item(self, item, spider):
	# 	print('Putting Person in Neo4J: ' + item['person_id'])
	# 	person_node = self.graph.merge_one("Person","id",item['person_id'])
	# 	person_node.properties['name'] = item['person']
	# 	person_node.push()
	# 	for film in item['films']:
	# 		film_node = self.graph.merge_one("Film","id",film)
	# 		film_node.properties['name'] = item['films'][film]
	# 		film_node.push()
	# 		self.graph.create_unique(Relationship(person_node,"ACTED_IN",film_node))
	# 	return item 



