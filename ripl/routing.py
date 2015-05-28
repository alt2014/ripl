#!/usr/bin/env python
'''@package routing

Routing engine base class.

@author Brandon Heller (brandonh@stanford.edu)
'''
from random import choice

import time
import logging
lg = logging.getLogger('ripl.routing')

DEBUG = True

lg.setLevel(logging.WARNING)
if DEBUG:
    lg.setLevel(logging.DEBUG)
    lg.addHandler(logging.StreamHandler())


class Routing(object):
    '''Base class for data center network routing.

    Routing engines must implement the get_route() method.
    '''

    def __init__(self, topo):
        '''Create Routing object.

        @param topo Topo object from Net parent
        '''
        self.topo = topo

    def get_route(self, src, dst, pkt):
        '''Return flow path.

        @param src source host
        @param dst destination host
        @param hash_ hash value

        @return flow_path list of DPIDs to traverse (including hosts)
        '''
        raise NotImplementedError


class StructuredRouting(Routing):
    '''Route flow through a StructuredTopo and return one path.

    Optionally accepts a function to choose among the set of valid paths.  For
    example, this could be based on a random choice, hash value, or
    always-leftmost path (yielding spanning-tree routing).

    Completely stupid!  Think of it as a topology-aware Dijstra's, that either
    extends the frontier until paths are found, or quits when it has looked for
    path all the way up to the core.  It simply enumerates all valid paths and
    chooses one.  Alternately, think of it as a bidrectional DFS.

    This is in no way optimized, and may be the slowest routing engine you've
    ever seen.  Still, it works with both VL2 and FatTree topos, and should
    help to bootstrap hardware testing and policy choices.

    The main data structures are the path dicts, one each for the src and dst.
    Each path dict has node ids as its keys.  The values are lists of routes,
    where each route records the list of dpids to get from the starting point
    (src or dst) to the key.

    Invariant: the last element in each route must be equal to the key.
    '''

    def __init__(self, topo, path_choice):
        '''Create Routing object.

        @param topo Topo object
        @param path_choice path choice function (see examples below)
        '''
        self.topo = topo
        self.path_choice = path_choice
        self.src_paths = None
        self.dst_paths = None
        self.src_path_layer = None
        self.dst_path_layer = None

    def _is_subpath(self, p1, p2):
        '''
        Test if p1 is a subpath of p2
            >>> _is_subpath([1,2], [1,2,3])
                True

            >>> _is_subpath([1,2,3], [1,2,3])
                True

            >>> _is_subpath([], [1,2,3])
                True

            >>> _is_subpath([1,3], [1,2,3])
                False
        '''
        return ','.join(map(str, p1)) in ','.join(map(str, p2))

    def _accept_candidate_path(self, reached, candidate_path):
        '''
        Accepting a path means it's "new". We accept iff:
            This path does not form a cycle
          AND
              We have no other path to this node
            OR
                This path is not a subpath of any other
              AND
                This is the shortest path from src -> dst
        '''
        b = candidate_path[-1]
        if b in reached:
            for path in reached[b]:
                if len(path) < len(candidate_path):
                    return False
                if self._is_subpath(candidate_path, path) or self._is_subpath(path, candidate_path):
                    return False

            return True
        else:
            return True

    # IAW FIXME: Recomputing these paths every time is dumb. Add an instance variable to the StructuredRouting
    # class that is a dictionary of these shortest routes from each node. Populate it once, at construction
    # time in __init__. Then get_route can just consult those already-computed paths.
    #
    # That won't work for switch or link failures, when routes need to be recomputed, but for our project we
    # probably don't care.
    def _get_all_shortest_routes(self, src):
        '''
        Populate a dictionary of all nodes reachable from 'src', each mapped to all non-cylic paths to them.
        Example:
                      B
                    /   \\
                   A     D
                    \\  /
                      C
            >>> _get_all_routes(A)
                {
                  B: [[A, B]],
                  C: [[A, C]],
                  D: [[A, B, D], [A, C, D]]
                }
        '''
        # Keys are all the nodes we've reached from src, value is list of paths to that node
        reached = {src: [[src]]}
        changed = True
        while changed:
            lg.debug("get_all_routes(%s)=" % str(src))
            changed = False
            reached_ = reached.copy()
            # For every node we've gotten to
            for node in reached_:
                if changed:
                    break

                # Consider all edges out from this node
                edges = self.topo.up_edges(node) + self.topo.down_edges(node)
                for edge in edges:
                    if changed:
                        break

                    (a, b) = edge
                    assert a == node

                    # For every path up this node, try extending to b
                    for path in reached[node]:
                        if changed:
                            break

                        candidate_path = path + [b]
                        if self._accept_candidate_path(reached, candidate_path):
                            changed = True
                            if b in reached:
                                if len(reached[b][0]) == len(candidate_path):
                                    # Equal lengths: append
                                    reached[b].append(candidate_path)
                                elif len(candidate_path) < len(reached[b][0]):
                                    # Shorter: replace
                                    reached[b] = [candidate_path]

                            else:
                                reached[b] = [candidate_path]

        return reached

    def get_route(self, src, dst, hash_):
        '''Return flow path.

        @param src source dpid (for host or switch)
        @param dst destination dpid (for host or switch)
        @param hash_ hash value

        @return flow_path list of DPIDs to traverse (including inputs), or None
        '''

        lg.debug("StructuredRouting get_route: src={}, dst={}, hash_={}".format(src, dst, hash_))

        if src == dst:
            return [src]

        reachable = self._get_all_shortest_routes(src)
        lg.debug("get_all_routes(%s)=" % str(src))
        lg.debug(reachable)

        if dst in reachable:
            path_choice = self.path_choice(reachable[dst], src, dst, hash_)
            lg.info('path_choice = %s' % path_choice)
            return path_choice
        else:
            return None

# Disable unused argument warnings in the classes below
# pylint: disable-msg=W0613


class STStructuredRouting(StructuredRouting):
    '''Spanning Tree Structured Routing.'''

    def __init__(self, topo):
        '''Create StructuredRouting object.

        @param topo Topo object
        '''

        def choose_leftmost(paths, src, dst, hash_):
            '''Choose leftmost path

            @param path paths of dpids generated by a routing engine
            @param src src dpid (unused)
            @param dst dst dpid (unused)
            @param hash_ hash value (unused)
	    '''
            return paths[0]

        super(STStructuredRouting, self).__init__(topo, choose_leftmost)


class RandomStructuredRouting(StructuredRouting):
    '''Random Structured Routing.'''

    def __init__(self, topo):
        '''Create StructuredRouting object.

        @param topo Topo object
        '''

        def choose_random(paths, src, dst, hash_):
            '''Choose random path

            @param path paths of dpids generated by a routing engine
            @param src src dpid (unused)
            @param dst dst dpid (unused)
            @param hash_ hash value (unused)
            '''
            return choice(paths)

        super(RandomStructuredRouting, self).__init__(topo, choose_random)


class HashedStructuredRouting(StructuredRouting):
    '''Hashed Structured Routing.'''

    def __init__(self, topo):
        '''Create StructuredRouting object.

        @param topo Topo object
        '''

        def choose_hashed(paths, src, dst, hash_):
            '''Choose consistent hashed path

            @param path paths of dpids generated by a routing engine
            @param src src dpid
            @param dst dst dpid
            @param hash_ hash value
            '''
            choice = hash_ % len(paths)
            path = sorted(paths)[choice]
            return path

        super(HashedStructuredRouting, self).__init__(topo, choose_hashed)


class GlobalFirstFitRouting(Routing):
    """Global First Fit Routing"""

    def __init__(self, topo):
        self.topo = topo
        self.src_paths = {}

        # Compute all-pair shortest paths ONCE at initialization:
        start = time.time()
        lg.debug('Computing all-pairs shortest routes...')
        for node in topo.nodes():
            reachable = self._get_all_shortest_routes(node)
            self.src_paths[node] = reachable

        sec = time.time() - start
        lg.debug('GlobalFirstFitRouting init complete in %0.2fs!' % sec)

        # Keep track of allocations for each link in the path
        self.allocations = {}
        for link in topo.links():
            sorted_link = sorted(link)
            link_str = '%s,%s' % (sorted_link[0], sorted_link[1])
            self.allocations[link_str] = 0

    def get_route(self, src, dst, pkt):
        '''Return flow path.

        @param src source host
        @param dst destination host
        @param hash_ hash value

        @return flow_path list of DPIDs to traverse (including hosts)
        '''
        dsts_to_paths = self.src_paths[src]
        if dst in dsts_to_paths:
            paths = dsts_to_paths[dst]
            min_alloc = None
            best_path = None

            # Select the path with lowest maximum allocation
            for path in paths:
                max_path_alloc = self._max_path_alloc(path)
                if min_alloc is None or max_path_alloc < min_alloc:
                    # Hold onto this path as our best candidate
                    min_alloc = max_path_alloc
                    best_path = path

            # Update link allocations to reflect our choice
            self._update_allocations(path)

            return best_path

        else:
            lg.warn('No path exists from %s to %s!\n' % (src, dst))
            return None

    def _update_allocations(self, path):
        """
        Increment the allocations for each link along the path.
        """
        prev_node = None
        for node in path:
            if prev_node is not None:
                (a, b) = sorted((prev_node, node))
                link_str = '%s,%s' % (a, b)
                self.allocations[link_str] += 1

            prev_node = node

    def _max_path_alloc(self, path):
        """
        Return the maximum current allocation for any link along this path.
        """
        max_ = -1
        prev_node = None
        for node in path:
            if prev_node is not None:
                (a, b) = sorted((prev_node, node))
                link_str = '%s,%s' % (a, b)
                if self.allocations[link_str] > max_:
                    max_ = self.allocations[link_str]

            prev_node = node

        return max_

    def _is_subpath(self, p1, p2):
        '''
        Test if p1 is a subpath of p2
            >>> _is_subpath([1,2], [1,2,3])
                True

            >>> _is_subpath([1,2,3], [1,2,3])
                True

            >>> _is_subpath([], [1,2,3])
                True

            >>> _is_subpath([1,3], [1,2,3])
                False
        '''
        return ','.join(map(str, p1)) in ','.join(map(str, p2))

    def _accept_candidate(self, reached, candidate_path):
        '''
        Accepting a path means it's "new". We accept iff:
            This path does not form a cycle
          AND
              We have no other path to this node
            OR
                This path is not a subpath of any other
              AND
                This is the shortest path from src -> dst
        '''
        b = candidate_path[-1]
        if b in reached:
            for path in reached[b]:
                if len(path) < len(candidate_path):
                    return False
                if (self._is_subpath(candidate_path, path) or
                   self._is_subpath(path, candidate_path)):
                    return False

            return True
        else:
            return True

    def _get_all_shortest_routes(self, src):
        '''
        Populate a dictionary of all nodes reachable from 'src', each mapped to
        all non-cylic paths to them.
        Example:
                      B
                    /   \\
                   A     D
                    \\  /
                      C
            >>> _get_all_routes(A)
                {
                  B: [[A, B]],
                  C: [[A, C]],
                  D: [[A, B, D], [A, C, D]]
                }
        '''
        # Keys are all the nodes we've reached from src
        # value is list of paths to that node
        reached = {src: [[src]]}
        changed = True
        while changed:
            lg.debug("get_all_shortest_routes(%s)=" % str(src))
            changed = False
            reached_ = reached.copy()
            # For every node we've gotten to
            for node in reached_:
                if changed:
                    break

                # Consider all edges out from this node
                edges = self.topo.up_edges(node) + self.topo.down_edges(node)
                for edge in edges:
                    if changed:
                        break

                    (a, b) = edge
                    assert a == node

                    # For every path up this node, try extending to b
                    for path in reached[node]:
                        if changed:
                            break

                        candidate_path = path + [b]
                        if self._accept_candidate(reached, candidate_path):
                            changed = True
                            if b in reached:
                                if len(reached[b][0]) == len(candidate_path):
                                    # Equal lengths: append
                                    reached[b].append(candidate_path)
                                elif len(candidate_path) < len(reached[b][0]):
                                    # Shorter: replace
                                    reached[b] = [candidate_path]

                            else:
                                reached[b] = [candidate_path]

        return reached

# pylint: enable-msg=W0613
