# -*- coding: utf-8 -*-
"""
Run this as a `zopectl run` script via for example:

  $ bin/instance run catalogoptimize.py

Note that it does actual transaction commits.
"""
from Acquisition import aq_base
from BTrees.IOBTree import IOBTree
from BTrees.OOBTree import OOBTree
from datetime import datetime
from Products.ZCatalog.ZCatalog import ZCatalog
from Products.ZCTextIndex.Lexicon import Lexicon
from Products.ZCTextIndex.ZCTextIndex import ZCTextIndex

import transaction


class Plone(object):

    combined = 0

    def __init__(self, site):
        self.site = site
        self.site_id = site.getId()

    @classmethod
    def is_site(cls, site_obj):
        return site_obj.meta_type == 'Plone Site'

    def optimize(self):
        now = datetime.now().isoformat()
        print('{0} - Starting for site "{1}" ...'.format(now, self.site_id))

        for zcatalog_obj in self.site.values():
            if PloneCatalog.is_zcatalog(zcatalog_obj):
                catalog = PloneCatalog(zcatalog_obj)
                catalog.optimize()
                self.combined += catalog.combined

        print(
            'Optimized away {0} buckets for site "{1}"'.format(
                self.combined,
                self.site_id,
            )
        )


class PloneCatalog(object):

    combined = 0

    def __init__(self, zcatalog):
        self.zcatalog = zcatalog
        self.zcatalog_id = zcatalog.getId()
        self.catalog = zcatalog._catalog

    @classmethod
    def is_zcatalog(self, zcatalog_obj):
        return isinstance(zcatalog_obj, ZCatalog)

    def optimize(self):
        now = datetime.now().isoformat()
        print('%s - Optimizing "%s"' % (now, self.zcatalog_id))

        self.optimize_structure()
        self.optimize_lexica()
        self.optimize_indexes()

    def optimize_structure(self):
        # optimize paths, uids, data - skip data for portal_catalog
        is_portal_catalog = self.zcatalog_id == 'portal_catalog'
        self.optimize_global_object(
            obj=self.catalog,
            no_data=is_portal_catalog
        )

    def optimize_lexica(self):
        for obj in self.zcatalog.values():
            if isinstance(obj, Lexicon):
                self.optimize_global_object(obj)

    def optimize_indexes(self):
        for index in self.catalog.indexes.values():
            if isinstance(index, ZCTextIndex):
                self.optimize_global_object(index.index)
            else:
                self.optimize_global_object(index)

    def optimize_global_object(self, obj, no_data=False):
        obj = aq_base(obj)
        result = 0
        obj._p_activate()
        for key, value in obj.__dict__.items():
            if no_data and key == 'data':
                # data blows up memory too much
                continue
            tree = Tree(obj, key, value)
            tree.optimize()
            result += tree.combined
            # handle sets inside *OBTrees
            if isinstance(value, (IOBTree, OOBTree)):
                obj._p_activate()
                new_value = obj.__dict__[key]
                for key2, value2 in new_value.iteritems():
                    tree = Tree(new_value, key2, value2, attributes=False)
                    tree.optimize()
                    result += tree.combined

        print('Optimized away {0} buckets in {1}'.format(result, obj))
        self.combined += result


class Tree(object):

    combined = 0

    def __init__(self, parent, key, value, attributes=True):
        self.parent = parent
        self.key = key
        self.value = value
        self.attributes = attributes

    def optimize(self):
        transaction.begin()
        bucket = getattr(self.value, '_firstbucket', None)
        if bucket is None:
            return

        readCurrent = getattr(bucket._p_jar, 'readCurrent', None)
        if readCurrent is not None:
            track_objects = True
        else:
            track_objects = False
        before_distribution, objects = self.blen(bucket,
                                            track_objects=track_objects)

        # do we have bucket lengths more than one which exist and aren't 90% full?
        # we assume here that 90% is one of 27, 54 or 108
        try:
            unoptimized = any(
                [a % 9 for a, b in before_distribution.items() if b > 1])
        except NameError:
            # Python 2.4 doesn't have any, we'll just loop over all items
            unoptimized = bool(
                [a % 9 for a, b in before_distribution.items() if b > 1])

        if not unoptimized:
            conn = self.parent._p_jar
            if conn:
                conn.cacheGC()
            transaction.abort()
            return

        # Gather stats used to figure out modfactor
        before = sum(before_distribution.values())
        maxsize = self.get_max_bucket_size(self.value)
        averagesize = sum([kk * vv for kk, vv in
                           before_distribution.items()]) * 1.0 / before
        bucketsizes = [x for sublist in [(kk,) * vv for kk, vv in
                                         sorted(
                                             before_distribution.items())]
                       for x in sublist]
        median = bucketsizes[before / 2]

        # Filling the tree in a two-step process. The first time we set up the tree,
        # values are inserted sequentially, resulting in 50% fill rate.
        # The second time we fill up with additional values to get fill rate higher
        # than 50%.
        # We want to set optimal fill rates based on current fill rate.
        # Fill rates of 55% or below indicates sequential index like dateindex
        # and we want 100% fill rate, otherwise 90% is good.
        avgrate = float(averagesize) / maxsize
        medianrate = float(median) / maxsize
        if avgrate < 0.55 or medianrate < 0.55 or medianrate > 0.95:
            modfactor = 2  # same number of items in both runs gives 100% fill
        else:
            modfactor = 9  # 5 in first run and 4 in second run gives 90% fill rate

        new = self.new_tree(self.value, modfactor)
        after_distribution, _ = self.blen(new._firstbucket)
        after = sum(after_distribution.values())
        if after < before:
            if readCurrent is not None:
                for obj in objects:
                    readCurrent(obj)
            if self.attributes:
                setattr(self.parent, self.key, new)
            else:
                self.parent[self.key] = new
            self.parent._p_changed = True
            many_buckets = {}
            few_buckets = []
            for k, v in after_distribution.items():
                if v > 1:
                    many_buckets[k] = v
                else:
                    few_buckets.append(k)
            newaveragesize = sum([kk * vv for kk, vv in
                                  after_distribution.items()]) * 1.0 / after
            newavgrate = float(newaveragesize) / maxsize
            print(
            'New buckets {fill size: count}: %s\nSingle buckets: %s\nfill: before %.3f after %.3f' % (
                str(many_buckets), str(few_buckets), avgrate,
                newavgrate))
            transaction.commit()
            self.combined = before - after

    def blen(self, bucket, track_objects=False):
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
        return distribution, objects

    def get_max_bucket_size(self, data):
        # Data is tree or treeset.
        # We calculate instead of hardcoding because values can be patched.
        tmp = data.__class__()
        if hasattr(tmp, 'items'):
            update = lambda x: (x, x)
        else:
            update = lambda x: x
        count = 0
        tmp.update([update(count)])
        bucket = tmp._firstbucket
        while bucket._next is None:
            count += 1
            tmp.update([update(count)])
        # Buckets are split on count
        return count

    def new_tree(self, old_tree, modfactor=9):
        # Fill the tree in a two-step process, which should result in better
        # fill rates
        klass = old_tree.__class__
        new = klass()
        count = 0
        tmp = []
        # If the last bucket is not 50% full after first run (it is fuller), it is likely
        # to split on second run, and the last 3 buckets will have lower fill rates,
        # instead of just the last one.
        # Idea: keep the tmp the same size as max and start on 2nd run inbetween 1st run
        # but with a max size delay
        if hasattr(old_tree, 'items'):
            # BTree
            for k, v in old_tree.items():
                modcount = count % modfactor
                if modcount % 2 == 0:
                    new[k] = v
                else:
                    tmp.append((k, v))
                count += 1
        else:
            # Tree set
            for k in old_tree.keys():
                modcount = count % modfactor
                if modcount % 2 == 0:
                    new.insert(k)
                else:
                    tmp.append(k)
                count += 1

        # Before adding the rest of the data, we need to make sure the last bucket
        # is not more than 50% full.
        # Add and remove synthetic values to provoke a bucket split
        maxsize = self.get_max_bucket_size(new)
        maxkey = new.maxKey()
        if isinstance(maxkey, int):
            synthetic = range(maxkey + 1, maxkey + 2 + (
            maxsize - self.get_bucket_sizes(new._firstbucket)[-1]))
        elif isinstance(maxkey, basestring):
            synthetic = [maxkey + str(x) for x in range(
                (maxsize - self.get_bucket_sizes(new._firstbucket)[-1]) + 1)]
        else:
            synthetic = []

        if hasattr(new, 'items'):
            for s in synthetic:
                new[s] = 0
            for s in synthetic:
                del new[s]
        else:
            for s in synthetic:
                new.insert(s)
            for s in synthetic:
                new.remove(s)

        # Add the rest of the data
        new.update(tmp)

        # Verify data
        assert len(old_tree) == len(new)
        return new

    def get_bucket_sizes(self, bucket):
        sizes = []
        while bucket is not None:
            sizes.append(len(bucket))
            bucket = bucket._next
        return sizes


###
# main
###

# Loop over all Plone sites
for site_obj in app.values():
    if Plone.is_site(site_obj):
        plone = Plone(site_obj)
        plone.optimize()

print('{0} - Finishing...'.format(datetime.now().isoformat()))
transaction.commit()
