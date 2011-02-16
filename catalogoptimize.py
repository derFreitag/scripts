import transaction
from Acquisition import aq_base
from BTrees.IOBTree import IOBTree
from BTrees.OOBTree import OOBTree
from Products.ZCatalog.ZCatalog import ZCatalog
from Products.ZCTextIndex.Lexicon import Lexicon
from Products.ZCTextIndex.ZCTextIndex import ZCTextIndex


def blen(bucket):
    distribution = {}
    while True:
        bucket_len = len(bucket)
        if distribution.get(bucket_len, None):
            distribution[bucket_len] += 1
        else:
            distribution[bucket_len] = 1
        bucket = bucket._next
        if bucket is None:
            break
    return distribution


def optimize_tree(parent, k, v, attr=True):
    bucket = getattr(v, '_firstbucket', None)
    if bucket is None:
        return 0
    result = 0
    before = sum(blen(bucket).values())
    klass = v.__class__

    # Fill the tree in a two-step process, which should result in better
    # fill rates
    new = klass()
    count = 0
    tmp = []
    if hasattr(v, 'items'):
        # BTree
        for kk,vv in v.items():
            if count % 2 == 0:
                new[kk] = vv
            else:
                tmp.append((kk,vv))
            count += 1
    else:
        # Tree set
        for kk in v.keys():
            if count % 2 == 0:
                new.insert(kk)
            else:
                tmp.append(kk)
            count += 1
    # Add the rest of the data
    new.update(tmp)

    # Verify data
    assert len(v) == len(new)

    after_distribution = blen(new._firstbucket)
    after = sum(after_distribution.values())
    if after < before:
        if attr:
            setattr(parent, k, new)
        else:
            parent[k] = new
        parent._p_changed = True
        result += before - after
        print('New buckets {fill size: count}: %s' % str(after_distribution))
        transaction.commit()
    else:
        conn = parent._p_jar
        if conn:
            conn.cacheGC()
    return result


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
