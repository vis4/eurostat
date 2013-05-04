import json
import os.path
import csv

tree = dict(id='', name='', children=[], level=-1)
toc = csv.reader(open('../toc/toc-en.txt'), delimiter='\t')
toc.next()

lnode = tree
_level = -1


def add_child(p, n):
   if 'children' not in p:
      p['children'] = []
   p['children'].append(n)
   n['parent'] = p


for t in toc:
   tname = t[0]
   level = (len(tname) - len(tname.lstrip())) / 4
   node = dict(id=t[1], name=tname.lstrip(), type=t[2], level=level)
   if level == lnode['level']:  # same level as last node
      add_child(lnode['parent'], node)
   elif level == lnode['level'] + 1:
      add_child(lnode, node)
   elif level < lnode['level']:
      while level <= lnode['level']:
         lnode = lnode['parent']
      add_child(lnode, node)
   lnode = node


# export tree
json_tree = {}

def export_tree(n):
   node = dict(name=n['name'], id=n['id'])
   if 'children' in n:
      node['children'] = []
      for c in n['children']:
         node['children'].append(export_tree(c))
   else:
      chk = ['tsv/%s.tsv' % n['id'], 'tsv/_imported/%s.tsv' % n['id']]
      for fn in chk:
         if os.path.exists(fn):
            node['size'] = os.path.getsize(fn)
   return node

json_tree = export_tree(tree)
open('tree.json', 'w').write(json.dumps(json_tree))
