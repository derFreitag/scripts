"""
Run this as a `zopectl run` script via for example:

  $ bin/instance run catalogoptimize.py

Note that it does actual transaction commits.
"""

import transaction
from Acquisition import aq_base
from BTrees.IOBTree import IOBTree
from BTrees.OOBTree import OOBTree
from Products.ZCatalog.ZCatalog import ZCatalog
from Products.ZCTextIndex.Lexicon import Lexicon
from Products.ZCTextIndex.ZCTextIndex import ZCTextIndex


def blen(bucket, track_objects=False):
    distribution = {}
    objects = []
    while True:
        bucket_len = len(bucket)
        if distribution.get(bucket_len, None):
            distribution[bucket_len] += 1
        else:
            distribution[bucket_len] = 1
        if track_objects:
            objects.append(bucket)
        bucket = bucket._next
        if bucket is None:
            break
    return (distribution, objects)


def new_tree(old_tree):
    # Fill the tree in a two-step process, which should result in better
    # fill rates
    klass = old_tree.__class__
    new = klass()
    count = 0
    tmp = []
    if hasattr(old_tree, 'items'):
        # BTree
        for k, v in old_tree.items():
            # We aim for 90% fill rate to avoid too many immediate splits
            modcount = count % 9
            if modcount % 2 == 0:
                new[k] = v
            else:
                tmp.append((k, v))
            count += 1
    else:
        # Tree set
        for k in old_tree.keys():
            if count % 2 == 0:
                new.insert(k)
            else:
                tmp.append(k)
            count += 1
    # Add the rest of the data
    new.update(tmp)

    # Verify data
    assert len(old_tree) == len(new)
    return new


def optimize_tree(parent, k, v, attr=True):
    transaction.begin()
    bucket = getattr(v, '_firstbucket', None)
    if bucket is None:
        return 0
    readCurrent = getattr(bucket._p_jar, 'readCurrent', None)
    if readCurrent is not None:
        track_objects = True
    else:
        track_objects = False
    before_distribution, objects = blen(bucket, track_objects=track_objects)

    # do we have bucket lengths more than one which exist and aren't 90% full?
    # we assume here that 90% is one of 27, 54 or 108
    unoptimized = any([k % 9 for k, v in before_distribution.items() if v > 1])

    if unoptimized:
        new = new_tree(v)
        after_distribution, _ = blen(new._firstbucket)
        after = sum(after_distribution.values())
        before = sum(before_distribution.values())
        if after < before:
            if readCurrent is not None:
                for obj in objects:
                    readCurrent(obj)
            if attr:
                setattr(parent, k, new)
            else:
                parent[k] = new
            parent._p_changed = True
            many_buckets = {}
            few_buckets = []
            for k, v in after_distribution.items():
                if v > 1:
                    many_buckets[k] = v
                else:
                    few_buckets.append(k)
            print('New buckets {fill size: count}: %s\nSingle buckets: %s' % (
                str(many_buckets), str(few_buckets)))
            transaction.commit()
            return before - after

    conn = parent._p_jar
    if conn:
        conn.cacheGC()
    transaction.abort()
    return 0


def optimize(obj, no_data=False):
    obj = aq_base(obj)
    result = 0
    obj._p_activate()
    for k, v in obj.__dict__.items():
        if no_data and k == 'data':
            # data blows up memory too much
            continue
        result += optimize_tree(obj, k, v)
        # handle sets inside *OBTrees
        if isinstance(v, (IOBTree, OOBTree)):
            obj._p_activate()
            new_v = obj.__dict__[k]
            for k2, v2 in new_v.iteritems():
                result += optimize_tree(new_v, k2, v2, attr=False)
    print('Optimized away %s buckets in %s' % (result, obj))
    return result


# Loop over all Plone sites
for site in app.values():
    if not site.meta_type == 'Plone Site':
        continue

    site_id = site.getId()
    print('Starting catalog optimization for site "%s" ...' % site_id)
    combined = 0
    for zcatalog in site.values():
        if not isinstance(zcatalog, ZCatalog):
            continue
        zcatalog_id = zcatalog.getId()
        print('Optimizing "%s"' % zcatalog_id)
        catalog = zcatalog._catalog
        # optimize paths, uids, data - skip data for portal_catalog
        combined += optimize(catalog, no_data=zcatalog_id == 'portal_catalog')
        # optimize lexica
        for obj in zcatalog.values():
            if isinstance(obj, Lexicon):
                combined += optimize(obj)
        # optimize indexes
        for index in catalog.indexes.values():
            if isinstance(index, ZCTextIndex):
                combined += optimize(index.index)
            else:
                combined += optimize(index)
    print('Optimized away %s buckets for site "%s"' % (combined, site_id))

print('Finishing...')
transaction.commit()
