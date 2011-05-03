"""
Run this as a `zopectl run` script via for example:

  $ bin/instance run analyze_inverted_index_potential.py

This will print out information about how many values and persistent objects
might be saved by storing inverted value sets for boolean, field and keyword
indexes.
"""

from pprint import pprint
from BTrees.IIBTree import IITreeSet

sites = app.objectValues('Plone Site')
site = sites[0]
catalog = site.portal_catalog._catalog
conn = site._p_jar

saved = 0
for index_id, index in catalog.indexes.items():
    _index = getattr(index, '_index', None)
    _unindex = getattr(index, '_unindex', None)
    if _unindex is None or _index is None:
        continue
    print(index_id)
    index_len = len(_unindex)
    results = []
    if isinstance(_index, IITreeSet):
        value_len = len(_index)
        percent = value_len * 100 / index_len
        if percent > 40:
            results.append(('%d' % percent, 'True'))
        if percent > 50:
            saved += value_len - (index_len - value_len)
    else:
        for k, v in _index.items():
            value_len = isinstance(v, int) and 1 or len(v)
            percent = value_len * 100 / index_len
            if percent > 40:
                results.append(('%d' % percent, k))
            if percent > 50:
                saved += value_len - (index_len - value_len)
    pprint(dict(sorted(results)))
    conn.cacheMinimize()

print('Possibly saved values: %s' % saved)
print('Possibly saved persisent objects (assuming 90%% fill rate on '
    'IITreeSets): %d' % (saved / 0.9 / 120))
