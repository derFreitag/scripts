# -*- coding: utf-8 -*-
"""
Run this as a `zopectl run` script via for example:

  $ bin/instance run catalogoptimize.py

Note that it does actual transaction commits.
"""
from Acquisition import aq_base
from BTrees.IOBTree import IOBTree
from BTrees.OOBTree import OOBTree
from collections import defaultdict
from datetime import datetime
from Products.ZCatalog.ZCatalog import ZCatalog
from Products.ZCTextIndex.Lexicon import Lexicon
from Products.ZCTextIndex.ZCTextIndex import ZCTextIndex

import sys
import transaction


class Main(object):

    plone_sites = []
    catalogs = []

    plone_site_filter = None
    catalog_filter = None
    index_filter = None

    def __init__(self, app):
        self.app = app
        self.set_filtering()

    def set_filtering(self):
        script_position = sys.argv.index('catalogoptimize.py')
        filters = sys.argv[script_position + 1:]

        try:
            self.plone_site_filter = filters[0]
            self.catalog_filter = filters[1]
            self.index_filter = filters[2]
        except IndexError:
            pass

    def run(self):
        self.gather_catalogs()
        self.optimize_catalogs()
        self.debug('Finishing...')
        transaction.commit()

    def gather_catalogs(self):
        self.debug('Gathering information', header=True)
        self.debug_filter_information()
        self.get_plone_sites()
        self.debug_info_about_plone_sites()
        self.get_catalogs_on_plone_sites()

    def debug_filter_information(self):
        if not self.plone_site_filter:
            return
        self.debug(
            'Filters set for website: {0} catalog: {1} index: {2}'.format(
                self.plone_site_filter,
                self.catalog_filter,
                self.index_filter,
            )
        )

    def get_plone_sites(self):
        plone_sites = [Plone(p) for p in Plone.get_sites(self.app)]
        if self.plone_site_filter:
            plone_sites = [
                p
                for p in plone_sites
                if p.site_id == self.plone_site_filter
            ]
        self.plone_sites = plone_sites

    def debug_info_about_plone_sites(self):
        site_ids = [p.site_id for p in self.plone_sites]
        self.debug(
            'Found {0} plone sites: {1}'.format(
                len(self.plone_sites),
                site_ids,
            )
        )

    def get_catalogs_on_plone_sites(self):
        catalogs = []
        for plone in self.plone_sites:
            self.debug('Plone {0}'.format(plone.site_id))

            tmp_catalogs = [
                PloneCatalog(c, index_filter=self.index_filter)
                for c in
                plone.get_catalogs()
            ]
            if self.catalog_filter:
                tmp_catalogs = [
                    c
                    for c in tmp_catalogs
                    if c.zcatalog_id == self.catalog_filter
                ]
            catalogs += tmp_catalogs
            self.debug_info_about_catalogs(plone, tmp_catalogs)

        self.catalogs = catalogs

    def debug_info_about_catalogs(self, plone, catalogs):
        catalog_ids = [c.zcatalog_id for c in catalogs]
        self.debug(
            'Plone {0} found {1} catalogs: {2}'.format(
                plone.site_id,
                len(catalogs),
                catalog_ids,
            )
        )

    def optimize_catalogs(self):
        self.debug('Optimizing', header=True)
        for catalog in self.catalogs:
            catalog.optimize()

    @staticmethod
    def debug(msg, header=False):
        if not header:
            print('{0} - {1}'.format(datetime.now().isoformat(), msg))
            return
        print('------- ' * 3)
        print(msg)
        print('------- ' * 3)


class Plone(object):

    def __init__(self, site):
        self.site = site
        self.site_id = site.getId()

    @classmethod
    def get_sites(cls, app):
        for obj in app.values():
            if Plone.is_site(obj):
                yield obj

    @classmethod
    def is_site(cls, site_obj):
        return site_obj.meta_type == 'Plone Site'

    def get_catalogs(self):
        for obj in self.site.values():
            if PloneCatalog.is_zcatalog(obj):
                yield obj


class PloneCatalog(object):

    tree_batch_size = 50000

    def __init__(self, zcatalog, index_filter=None):
        self.zcatalog = zcatalog
        self.zcatalog_id = zcatalog.getId()
        self.catalog = zcatalog._catalog
        self.index_filter = index_filter

    @classmethod
    def is_zcatalog(cls, zcatalog_obj):
        return isinstance(zcatalog_obj, ZCatalog)

    def optimize(self):
        for obj in self.get_objects_to_optimize():
            for batch, processed in self.get_trees_in_object_batched(obj):
                result = 0
                for index, tree in enumerate(batch):
                    result += tree.optimize()
                self.debug(
                    'Optimized away {0} buckets on {1} trees'.format(
                        result,
                        processed,
                    )
                )

    def get_objects_to_optimize(self):
        objs = []
        if not self.index_filter:
            objs.append(self.catalog)
            objs += self._get_lexicons()

        objs += self._get_indexes()
        self._debug_objects_to_optimize(objs)
        return objs

    def _debug_objects_to_optimize(self, objects_to_optimize):
        self.debug(
            '{0} objects to optimize in {1}'.format(
                len(objects_to_optimize),
                self.zcatalog_id,
            )
        )

    def _get_lexicons(self):
        lexicons = [
            l
            for l in self.zcatalog.values()
            if isinstance(l, Lexicon)
        ]
        return lexicons

    def _get_indexes(self):
        indexes = []
        indexes_ids = self.catalog.indexes.keys()
        if self.index_filter:
            position = indexes_ids.index(self.index_filter)
            indexes_ids = indexes_ids[position:]

        for index_id in indexes_ids:
            index = self.catalog.indexes[index_id]
            if isinstance(index, ZCTextIndex):
                indexes.append(index.index)
            else:
                indexes.append(index)
        return indexes

    def get_trees_in_object_batched(self, obj):
        total_trees = 0
        trees = []
        obj_id = obj.id
        no_data = False
        if self.zcatalog_id == 'portal_catalog':
            no_data = True

        obj = aq_base(obj)
        obj._p_activate()
        self.debug('-- Trees in {0} : {1}'.format(obj_id, len(obj.__dict__) ))
        for key, value in obj.__dict__.items():
            if no_data and key == 'data':
                # data blows up memory too much
                continue

            tree = Tree(obj, key, value)
            trees.append(tree)
            total_trees += 1
            if self.has_to_process_batch(trees):
                yield trees, total_trees
                trees = []

            # handle sets inside *OBTrees
            if isinstance(value, (IOBTree, OOBTree)):
                obj._p_activate()
                new_value = obj.__dict__[key]
                self.debug(
                    '--- Get trees in {0} {1}: {2}'.format(
                        obj_id,
                        key,
                        len(new_value),
                    )
                )
                for key2, value2 in new_value.iteritems():
                    tree = Tree(new_value, key2, value2, attributes=False)
                    trees.append(tree)
                    total_trees += 1
                    if self.has_to_process_batch(trees):
                        yield trees, total_trees
                        trees = []
        yield trees

    def has_to_process_batch(self, batch):
        count = len(batch)
        return count > 1 and count % self.tree_batch_size == 0

    @staticmethod
    def debug(msg):
        print('{0} - {1}'.format(datetime.now().isoformat(), msg))


class Tree(object):

    def __init__(self, parent, key, btree, attributes=True):
        self.parent = parent
        self.key = key
        self.btree = btree
        self.attributes = attributes
        self.bucket = self.get_first_bucket()
        self.readCurrent = self.get_readCurrent_method()
        self.track_objects = bool(self.readCurrent)

    def get_first_bucket(self):
        return getattr(self.btree, '_firstbucket', None)

    def get_readCurrent_method(self):
        if self.bucket:
            return getattr(self.bucket._p_jar, 'readCurrent', None)
        return None

    def optimize(self):
        transaction.begin()
        if self.bucket is None:
            return 0

        before_distribution, objects = self.get_btree_information(
            self.bucket,
            track_objects=self.track_objects,
        )

        if self.is_optimized(before_distribution):
            conn = self.parent._p_jar
            if conn:
                conn.cacheGC()
            transaction.abort()
            return 0

        stats = self.gather_stats(before_distribution)
        before, maxsize, avgrate, modfactor = stats

        new = self.new_tree(self.btree, modfactor)
        after_distribution, _ = self.get_btree_information(new._firstbucket)
        after = sum(after_distribution.values())
        if after < before:
            if self.readCurrent is not None:
                for obj in objects:
                    self.readCurrent(obj)
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
                'New buckets {{fill size: count}}: {0}\n'
                'Single buckets: {1}\n'
                'fill: before {2:.3f} after {3:.3f}'.format(
                    str(many_buckets),
                    str(few_buckets),
                    avgrate,
                    newavgrate,
                )
            )
            transaction.commit()
            return before - after

        return 0

    def is_optimized(self, before_distribution):
        # do we have bucket lengths more than one which exist and aren't
        # 90% full?
        # we assume here that 90% is one of 27, 54 or 108
        return not any(
            [
                a % 9
                for a, b in before_distribution.items()
                if b > 1
            ]
        )

    def gather_stats(self, before_distribution):
        # Gather stats used to figure out modfactor
        before = sum(before_distribution.values())
        maxsize = self.get_max_bucket_size(self.btree)
        totalsize = sum(
            [
                kk * vv
                for kk, vv in
                before_distribution.items()
            ]
        )
        averagesize = float(totalsize) / before
        bucketsizes = [
            x
            for sublist in
            [
                (kk,) * vv
                for kk, vv in sorted(before_distribution.items())
            ]
            for x in sublist
        ]
        median = bucketsizes[before / 2]

        # Filling the tree in a two-step process.
        # The first time we set up the tree,
        # values are inserted sequentially, resulting in 50% fill rate.
        # The second time we fill up with additional values to get fill
        # rate higher than 50%.
        # We want to set optimal fill rates based on current fill rate.
        # Fill rates of 55% or below indicates sequential index like dateindex
        # and we want 100% fill rate, otherwise 90% is good.
        avgrate = float(averagesize) / maxsize
        medianrate = float(median) / maxsize

        modfactor = 9  # 5 in first run and 4 in second run gives 90% fill rate
        if avgrate < 0.55 or medianrate < 0.55 or medianrate > 0.95:
            modfactor = 2  # same number of items in both runs gives 100% fill

        return before, maxsize, avgrate, modfactor

    def get_btree_information(self, bucket, track_objects=False):
        distribution = defaultdict(int)
        objects = []
        while True:
            bucket_len = len(bucket)
            distribution[bucket_len] += 1
            if track_objects:
                objects.append(bucket)
            bucket = bucket._next
            if bucket is None:
                break
        return distribution, objects

    def new_tree(self, old_tree, modfactor=9):
        # Fill the tree in a two-step process, which should result in better
        # fill rates
        klass = old_tree.__class__
        new = klass()
        count = 0
        tmp = []
        # If the last bucket is not 50% full after first run (it is fuller),
        # it is likely to split on second run,
        # and the last 3 buckets will have lower fill rates,
        # instead of just the last one.
        # Idea: keep the tmp the same size as max and start on 2nd run
        # inbetween 1st run but with a max size delay
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

    def get_bucket_sizes(self, bucket):
        sizes = []
        while bucket is not None:
            sizes.append(len(bucket))
            bucket = bucket._next
        return sizes


if __name__ == '__main__':
    main = Main(app)
    main.run()
