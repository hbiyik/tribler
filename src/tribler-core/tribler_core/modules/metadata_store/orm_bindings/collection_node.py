import os
from pathlib import Path

from ipv8.database import database_blob

from pony import orm
from pony.orm import db_session, select

from tribler_core.exceptions import DuplicateTorrentFileError
from tribler_core.modules.libtorrent.torrentdef import TorrentDef
from tribler_core.modules.metadata_store.discrete_clock import clock
from tribler_core.modules.metadata_store.orm_bindings.channel_metadata import chunks
from tribler_core.modules.metadata_store.orm_bindings.channel_node import (
    COMMITTED,
    DIRTY_STATUSES,
    LEGACY_ENTRY,
    NEW,
    TODELETE,
    UPDATED,
)
from tribler_core.modules.metadata_store.orm_bindings.torrent_metadata import tdef_to_metadata_dict
from tribler_core.modules.metadata_store.serialization import COLLECTION_NODE, CollectionNodePayload
from tribler_core.utilities.random_utils import random_infohash


def define_binding(db):
    class CollectionNode(db.MetadataNode):
        """
        This ORM class represents a generic named container, i.e. a folder. It is used as an intermediary node
        in building the nested channels tree.
        Methods for copying stuff recursively are bound to it.
        """

        _discriminator_ = COLLECTION_NODE

        # FIXME: ACHTUNG! PONY BUG! attributes inherited from multiple inheritance are not cached!
        # Therefore, we are forced to move the attributes to common ancestor class of CollectionNode and ChannelTorrent,
        # that is MetadataNode. When Pony fixes it, we must move it here for clarity.
        # num_entries = orm.Optional(int, size=64, default=0)

        # Special class-level properties
        _payload_class = CollectionNodePayload
        payload_arguments = _payload_class.__init__.__code__.co_varnames[
            : _payload_class.__init__.__code__.co_argcount
        ][1:]
        nonpersonal_attributes = db.MetadataNode.nonpersonal_attributes + ('num_entries',)

        @property
        @db_session
        def state(self):
            if self.is_personal:
                return "Personal"
            return "Preview"

        def to_simple_dict(self):
            result = super(CollectionNode, self).to_simple_dict()
            result.update(
                {"torrents": self.num_entries, "state": self.state, "dirty": self.dirty if self.is_personal else False}
            )
            return result

        def make_copy(self, tgt_parent_id, recursion_depth=15, **kwargs):
            new_node = db.MetadataNode.make_copy(self, tgt_parent_id, **kwargs)
            # Recursive copying
            if recursion_depth:
                for node in self.actual_contents:
                    if issubclass(type(node), CollectionNode):
                        node.make_copy(new_node.id_, recursion_depth=recursion_depth - 1)
                    else:
                        node.make_copy(new_node.id_)
            return new_node

        @db_session
        def copy_torrent_from_infohash(self, infohash):
            """
            Search the database for a given infohash and create a copy of the matching entry in the current channel
            :param infohash:
            :return: New TorrentMetadata signed with your key.
            """

            existing = db.TorrentMetadata.select(lambda g: g.infohash == database_blob(infohash)).first()

            if not existing:
                return None

            new_entry_dict = {
                "origin_id": self.id_,
                "infohash": existing.infohash,
                "title": existing.title,
                "tags": existing.tags,
                "size": existing.size,
                "torrent_date": existing.torrent_date,
                "tracker_info": existing.tracker_info,
                "status": NEW,
            }
            return db.TorrentMetadata.from_dict(new_entry_dict)

        @property
        def dirty(self):
            return self.contents.where(lambda g: g.status in DIRTY_STATUSES).exists()

        @property
        def contents(self):
            return db.ChannelNode.select(
                lambda g: g.public_key == self.public_key and g.origin_id == self.id_ and g != self
            )

        @property
        def actual_contents(self):
            return self.contents.where(lambda g: g.status != TODELETE)

        @db_session
        def get_random_contents(self, limit):
            return self.contents.where(lambda g: g.status not in [NEW, TODELETE]).random(limit)

        @property
        @db_session
        def contents_list(self):
            return list(self.contents)

        @property
        def contents_len(self):
            return orm.count(self.contents)

        @db_session
        def torrent_exists(self, infohash):
            """
            Return True if torrent with given infohash exists in the user channel
            :param infohash: The infohash of the torrent
            :return: True if torrent exists else False
            """
            return db.TorrentMetadata.exists(
                lambda g: g.public_key == self.public_key
                and g.infohash == database_blob(infohash)
                and g.status != LEGACY_ENTRY
            )

        @db_session
        def add_torrent_to_channel(self, tdef, extra_info=None):
            """
            Add a torrent to your channel.
            :param tdef: The torrent definition file of the torrent to add
            :param extra_info: Optional extra info to add to the torrent
            """
            new_entry_dict = dict(tdef_to_metadata_dict(tdef), status=NEW)
            if extra_info:
                new_entry_dict['tags'] = extra_info.get('description', '')

            # See if the torrent is already in the channel
            old_torrent = db.TorrentMetadata.get(public_key=self.public_key, infohash=tdef.get_infohash())
            if old_torrent:
                # If it is there, check if we were going to delete it
                if old_torrent.status == TODELETE:
                    new_timestamp = clock.tick()
                    old_torrent.set(timestamp=new_timestamp, origin_id=self.id_, **new_entry_dict)
                    old_torrent.sign()
                    # As we really don't know what status this torrent had _before_ it got its TODELETE status,
                    # we _must_ set its status to UPDATED, for safety
                    old_torrent.status = UPDATED
                    torrent_metadata = old_torrent
                else:
                    raise DuplicateTorrentFileError()
            else:
                torrent_metadata = db.TorrentMetadata.from_dict(dict(origin_id=self.id_, **new_entry_dict))
            return torrent_metadata

        @db_session
        def pprint_tree(self, file=None, _prefix="", _last=True):
            print(_prefix, "`- " if _last else "|- ", (self.num_entries, self.metadata_type), sep="", file=file)
            _prefix += "   " if _last else "|  "
            child_count = self.actual_contents.count()
            for i, child in enumerate(list(self.actual_contents)):
                if issubclass(type(child), CollectionNode):
                    _last = i == (child_count - 1)
                    child.pprint_tree(file, _prefix, _last)
                else:
                    print(_prefix, "`- " if _last else "|- ", child.metadata_type, sep="", file=file)

        @db_session
        def get_contents_recursive(self):
            results_stack = []
            for subnode in self.contents:
                if issubclass(type(subnode), CollectionNode):
                    results_stack.extend(subnode.get_contents_recursive())
                results_stack.append(subnode)
            return results_stack

        @db_session
        def add_torrents_from_dir(self, torrents_dir, recursive=False):
            torrents_list = []
            errors_list = []

            def rec_gen(dir_):
                for root, _, filenames in os.walk(dir_):
                    for fn in filenames:
                        yield Path(root) / fn

            filename_generator = rec_gen(torrents_dir) if recursive else os.listdir(torrents_dir)
            # Build list of .torrents to process
            torrents_list_generator = (Path(torrents_dir, f) for f in filename_generator)
            torrents_list = [f for f in torrents_list_generator if f.is_file() and f.suffix == ".torrent"]

            # 100 is a reasonable chunk size for commits
            for chunk in chunks(torrents_list, 100):
                for f in chunk:
                    try:
                        self.add_torrent_to_channel(TorrentDef.load(f))
                    except DuplicateTorrentFileError:
                        pass
                    except Exception:
                        # Have to use the broad exception clause because Py3 versions of libtorrent
                        # generate generic Exceptions
                        errors_list.append(f)
                # Optimization to drop excess cache
                orm.commit()

            return torrents_list, errors_list

        @staticmethod
        @db_session
        def commit_all_channels():
            committed_channels = []
            commit_queues_list = db.ChannelMetadata.get_commit_forest()
            for _, queue in commit_queues_list.items():
                channel = queue[-1]
                # Committing empty channels
                if len(queue) == 1:
                    # Empty top-level channels are deleted on-sight
                    if channel.status == TODELETE:
                        channel.delete()
                    else:
                        # Only the top-level channel entry was changed. Just mark it committed and do nothing.
                        channel.status = COMMITTED
                    continue

                # Committing non-empty channels
                queue_prepared = db.ChannelMetadata.prepare_commit_queue_for_channel(queue)
                if isinstance(channel, db.ChannelMetadata):
                    committed_channels.append(channel.commit_channel_torrent(commit_list=queue_prepared))
                # Top-level collections get special treatment.
                # These can be used for e.g. non-published personal favourites collections.
                elif isinstance(channel, db.CollectionNode):
                    for g in queue:
                        if g.status in [NEW, UPDATED]:
                            g.status = COMMITTED
                        elif g.status == TODELETE:
                            g.delete()

            return committed_channels

        @staticmethod
        @db_session
        def get_children_dict_to_commit():
            db.CollectionNode.collapse_deleted_subtrees()
            upd_dict = {}
            children = {}
            # TODO: optimize me by rewriting in pure SQL with recursive CTEs

            def update_node_info(n):
                # Add the node to its parent's set of children
                if n.origin_id not in children:
                    children[n.origin_id] = {n}
                else:
                    children[n.origin_id].add(n)
                upd_dict[n.id_] = n

            dead_parents = set()
            # First we traverse the tree upwards from changed leaves to find all nodes affected by changes
            for node in db.ChannelNode.select(
                lambda g: g.public_key == db.ChannelNode._my_key.pub().key_to_bin()[10:] and g.status in DIRTY_STATUSES
            ):
                update_node_info(node)
                # This process resolves the parents completely.
                # Therefore, if a parent is already in the dict, its path has already been resolved.
                while node and (node.origin_id not in upd_dict):
                    # Add the node to its parent's set of children
                    update_node_info(node)
                    # Get parent node
                    parent = db.CollectionNode.get(public_key=node.public_key, id_=node.origin_id)
                    if not parent:
                        dead_parents.add(node.origin_id)
                    node = parent

            # Normally, dead_parents should consist only of 0 node, which is root. Otherwise, we got some orphans.
            if 0 in dead_parents:
                dead_parents.remove(0)
            # Delete orphans
            db.ChannelNode.select(
                lambda g: database_blob(db.ChannelNode._my_key.pub().key_to_bin()[10:]) == g.public_key
                and g.origin_id in dead_parents
            ).delete()
            orm.flush()  # Just in case...
            if not children or 0 not in children:
                return {}
            return children

        def iter_nodes_to_commit(self, all=False):
            set_sql_debug(True, True)
            # optimize this, we dont need all columns, fewer the columns faster the sql execution
            # for cleanness keep row order same here even some rows are removed
            cols = ["rowid", "metadata_type", "reserved_flags", "origin_id", "public_key", "id_",
                    "signature", "added_on", "status", "title", "tags", "infohash", "size",
                    "torrent_date", "tracker_info"]
            columns = ", ".join(cols)
            # dont use db_session in this method since db_session is not awware of generators
            # be carefull about concurrency because we are using directly the cursor
            # concurrent access to cursor will yield OperationalError exception

            # below query selects the nodes with num_entries counted recursively, and timestamps updated
            # each num entries query is a top-down recursive query
            # timestamps are calcualted with 1 ms accuracy, channel root is the earliest, TO_DELETEs are the latest.
            # TO_DELETE entries are sorted to latest, the channels and collections are sorted to the first.
            # we dont use ? or $ operators because sqlite or pony does not seem to parse coorectly nested sql queries
            # there is no sanitastion, sql could be injected however our variables are static, should not be a problem

            select_with_count = """SELECT %s,
                                   CASE metadata_type WHEN %s THEN 0 ELSE (
                                       WITH recursive 
                                           CNT(id_, metadata_type) AS (
                                               SELECT id_, metadata_type FROM ChannelNode
                                               WHERE ChannelNode.id_ = NODES.id_
                                                   UNION ALL
                                               SELECT ChannelNode.id_, ChannelNode.metadata_type FROM ChannelNode, CNT
                                               WHERE ChannelNode.origin_id = CNT.id_ AND ChannelNode.status != %s
                                       ) SELECT count(id_) FROM CNT where metadata_type = %s 
                                           ) END as num_entries,
                                   (select COUNT() from NODES) as total
                                   FROM NODES ORDER BY status=%s, lvl DESC""" % (columns,
                                                                                 REGULAR_TORRENT,
                                                                                 TODELETE,
                                                                                 REGULAR_TORRENT,
                                                                                 TODELETE
                                                                                 )

            # Below query generates the pivot table that holds all to nodes to commit.
            # First we go from top to bottom to find all recursive elements of the node,
            # to seperate selection from all nodes
            # when go top to bottom, we dont check if the collection is in dirty because
            # their children may still be dirty for torrents we only select the dirty one
            # to optimize the recursive selection.
            # when going from top 2 bottom, union all is enough because there is no duplicate merge of the rows.
            # TIP: union all is much faster than pure union, since union all does not check the duplicates
            # Since we dont check the status of Nodes when going top2bottom,
            # we need to filter out only the torrent or nodes
            # which are in dirty states, for this, we go from bottom to the top
            # in the previous table we generated with top2bottom
            # thus we can only find the the nodes which are dirty. This tables requires UNION to prevent
            # duplicates generated for the same parent node that has multiple members.
            # Finally we select from the table with correct timestamp and num entries with the previous sql
            # generated by select_with_count, i know this complicated but fast as f***.

            # ACHTUNG: we dont check infinite loop cases where some bored guy can manipulate the actual db
            # this should be done on another sql where we clear orphans or illegal entries
            # we dont check recursiveness depth as of now, but i think we shouldnt in scalable db structure
            # still recusriveness depth is an important KPI for performance.

            nodecolumns = ",".join(["ChannelNode.%s" % x for x in cols])
            if all:
                rec_select = """WITH recursive
                                NODES(%s) AS (
                                    SELECT %s  FROM ChannelNode 
                                        WHERE ChannelNode.id_ = %s
                                            UNION ALL
                                        SELECT %s FROM ChannelNode, NODES
                                        WHERE ChannelNode.origin_id = NODES.id_
                                ) %s """ % (columns,
                                            columns,
                                            self._id,
                                            nodecolumns,
                                            select_with_count
                                            )
            else:
                rec_select = """WITH recursive
                                TOP2BOTTOM(%s) AS (
                                    SELECT %s  FROM ChannelNode 
                                        WHERE ChannelNode.id_ = %s
                                        AND (ChannelNode.status in %s OR ChannelNode.metadata_type != %s)
                                            UNION ALL
                                        SELECT %s FROM ChannelNode, TOP2BOTTOM
                                        WHERE ChannelNode.origin_id = TOP2BOTTOM.id_
                                ),
                                NODES(%s, lvl) AS (
                                        SELECT %s, 0 FROM TOP2BOTTOM
                                        WHERE TOP2BOTTOM.status in %s
                                            UNION
                                        SELECT TOP2BOTTOM.*, NODES.lvl + 1 as lvl from TOP2BOTTOM, NODES
                                        WHERE TOP2BOTTOM.id_ = NODES.origin_id
                                ) %s""" % (columns,
                                           columns,
                                           self.id_,
                                           DIRTY_STATUSES,
                                           REGULAR_TORRENT,
                                           nodecolumns,
                                           columns,
                                           columns,
                                           DIRTY_STATUSES,
                                           select_with_count
                                           )

            import time
            import sqlite3
            t1 = time.time()
            # We work directly with connection object to verify that Sqlite3.Row is the row factory
            # I dont think theres a cleaner way of doing it with pony
            # TO-DO: We can save some cpu cycles here by declaring PARSE_DECLTYPES on sqlite3.connect
            # instead of runtime conversion of datetime values returned from DB as string.
            # but dont think this is possible without modfying pony.
            cache = db._get_cache()
            connection = cache.prepare_connection_for_query_execution()
            if not hasattr(connection, "row_factory") or connection.row_factory is None:
                connection.row_factory = sqlite3.Row
            cur = connection.cursor()
            # only below line will block the reactor, and this is where the magic happens
            # and we outsource all kind of recursive cpu heavy operations to c level sqlite library
            # to prevent reactor block, the cursor can be called on a thread, however be carefull
            # about the concurrency!
            cur.execute(rec_select)
            print(time.time() - t1)

            # we deliberately iterate over each single row, to leverage low memory consumption
            # in python side, all the table should be in the memory of sqlite not tribler,
            # when we are done with the row, it is GC handled in py, *hopefully*
            # this way we remove 1MB limit for mdblob size limit.
            while True:
                rec = cur.fetchone()
                if not rec:
                    break
                yield rec

        @staticmethod
        @db_session
        def get_commit_forest():
            children = db.CollectionNode.get_children_dict_to_commit()
            if not children:
                return {}
            # We want a separate commit tree/queue for each toplevel channel
            forest = {}
            toplevel_nodes = [node for node in children.pop(0)]
            for root_node in toplevel_nodes:
                # Tree -> stack -> queue
                commit_queue = []
                tree_stack = [root_node]
                while tree_stack and children.get(tree_stack[-1].id_, None):
                    # Traverse the tree from top to bottom converting it to a stack
                    while children.get(tree_stack[-1].id_, None):
                        node = children[tree_stack[-1].id_].pop()
                        tree_stack.append(node)

                    while not issubclass(type(tree_stack[-1]), db.CollectionNode):
                        commit_queue.append(tree_stack.pop())
                    # Unwind the tree stack until either the stack is empty or we meet a non-empty node
                    while tree_stack and not children.get(tree_stack[-1].id_, None):
                        while not issubclass(type(tree_stack[-1]), db.CollectionNode):
                            commit_queue.append(tree_stack.pop())

                        # It was a terminal collection
                        collection = tree_stack.pop()
                        commit_queue.append(collection)

                if not commit_queue or commit_queue[-1] != root_node:
                    commit_queue.append(root_node)
                forest[root_node.id_] = tuple(commit_queue)

            return forest

        @staticmethod
        def prepare_commit_queue_for_channel(commit_queue):
            """
            This routine prepares the raw commit queue for commit by updating the elements' properties and
            re-signing them. Also, it removes the channel entry itself from the queue [:-1], because its
            meaningless to put it in the blobs, as it must be updated with the new infohash after commit.

            :param commit_queue:
            :return:
            """
            for node in commit_queue:
                # Avoid updating entries that must be deleted:
                # soft delete payloads require signatures of unmodified entries
                if issubclass(type(node), db.CollectionNode) and node.status != TODELETE:
                    # Update recursive count of actual non-collection contents
                    node.num_entries = select(
                        # For each subnode, if it is a collection, add the count of its contents to the recursive sum.
                        # Otherwise, add just 1 to the sum (to count the subnode itself).
                        (g.num_entries if g.metadata_type == COLLECTION_NODE else 1)
                        for g in node.actual_contents
                    ).sum()
                    node.timestamp = clock.tick()
                    node.sign()
            # This perverted comparator lambda is necessary to ensure that delete entries are always
            # sorted to the end of the list, as required by the channel serialization routine.
            return sorted(commit_queue[:-1], key=lambda x: int(x.status == TODELETE) - 1 / x.timestamp)

        def delete(self, *args, **kwargs):
            # Recursively delete contents
            if kwargs.pop('recursive', True):
                for node in self.contents:
                    node.delete(*args, **kwargs)
            super(CollectionNode, self).delete(*args, **kwargs)

        @staticmethod
        @db_session
        def collapse_deleted_subtrees():
            """
            This procedure scans personal channels for collection nodes marked TODELETE and recursively removes
            their contents. The top-level nodes themselves are left intact so soft delete entries can be generated
            in the future.
            This procedure should be always run _before_ committing personal channels.
            """
            # TODO: optimize with SQL recursive CTEs

            def get_highest_deleted_parent(node, highest_deleted_parent=None):
                if node.origin_id == 0:
                    return highest_deleted_parent
                parent = db.CollectionNode.get(public_key=node.public_key, id_=node.origin_id)
                if not parent:
                    return highest_deleted_parent
                if parent.status == TODELETE:
                    highest_deleted_parent = parent
                return get_highest_deleted_parent(parent, highest_deleted_parent)

            deletion_set = {
                get_highest_deleted_parent(node, highest_deleted_parent=node).rowid
                for node in db.CollectionNode.select(
                    lambda g: g.public_key == db.CollectionNode._my_key.pub().key_to_bin()[10:] and g.status == TODELETE
                )
                if node
            }

            for node in [db.CollectionNode[rowid] for rowid in deletion_set]:
                for subnode in node.contents:
                    subnode.delete()

        @db_session
        def get_contents_to_commit(self):
            return db.ChannelMetadata.prepare_commit_queue_for_channel(self.get_commit_forest().get(self.id_, []))

        def update_properties(self, update_dict):
            # Sanity checks: check that we don't create a recursive dependency or an orphaned channel
            new_origin_id = update_dict.get('origin_id', self.origin_id)
            if new_origin_id not in (0, self.origin_id):
                new_parent = CollectionNode.get(public_key=self.public_key, id_=new_origin_id)
                if not new_parent:
                    raise ValueError("Target collection does not exists")
                root_path = new_parent.get_parents_ids()
                if new_origin_id == self.id_ or self.id_ in root_path:
                    raise ValueError("Can't move collection into itself or its descendants!")
                if 0 not in root_path:
                    # TODO: add orphan-cleaning hook here
                    raise ValueError("Tried to move collection into an orphaned hierarchy!")
            updated_self = super(CollectionNode, self).update_properties(update_dict)
            if updated_self.origin_id == 0 and self.metadata_type == COLLECTION_NODE:
                # Coerce to ChannelMetadata
                # ACHTUNG! This is a somewhat awkward way to re-create the entry as an instance of
                # another class. Be very careful with it!
                self_dict = updated_self.to_dict()
                updated_self.delete(recursive=False)
                self_dict.pop("rowid")
                self_dict.pop("metadata_type")
                self_dict['infohash'] = random_infohash()
                self_dict["sign_with"] = self._my_key
                updated_self = db.ChannelMetadata.from_dict(self_dict)
            return updated_self

    return CollectionNode
