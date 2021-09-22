#!/usr/bin/python

# This is a dummy peer that just illustrates the available information your peers 
# have available.

# You'll want to copy this file to AgentNameXXX.py for various versions of XXX,
# probably get rid of the silly logging messages, and then add more logic.

import collections
import random
import logging

from messages import Upload, Request
from util import even_split
from peer import Peer
from collections import defaultdict

class TodoketeStd(Peer):
    def post_init(self):
        print(("post_init(): %s here!" % self.id))
        self.dummy_state = dict()
        self.dummy_state["cake"] = "lie"
        self.optimistic_unchoke = None
    
    def requests(self, peers, history):
        """
        peers: available info about the peers (who has what pieces)
        history: what's happened so far as far as this peer can see

        returns: a list of Request() objects

        This will be called after update_pieces() with the most recent state.
        """
        needed = lambda i: self.pieces[i] < self.conf.blocks_per_piece
        needed_pieces = list(filter(needed, list(range(len(self.pieces)))))
        np_set = set(needed_pieces)  # sets support fast intersection ops.


        logging.debug("%s here: still need pieces %s" % (
            self.id, needed_pieces))

        logging.debug("%s still here. Here are some peers:" % self.id)
        for p in peers:
            logging.debug("id: %s, available pieces: %s" % (p.id, p.available_pieces))

        logging.debug("And look, I have my entire history available too:")
        logging.debug("look at the AgentHistory class in history.py for details")
        logging.debug(str(history))

        requests = []   # We'll put all the things we want here
        
        # Sort peers randomly
        random.shuffle(peers)

        # Determine rarity of each piece (e.g. how many peers own each piece)
        rarity = defaultdict(int)
        for peer in peers:
            for piece_id in peer.available_pieces:
                rarity[piece_id] += 1

        # request all available pieces from all peers!
        # (up to self.max_requests from each)
        for peer in peers:
            av_set = set(peer.available_pieces)
            isect = av_set.intersection(np_set)
            isect = list(isect)
            
            n = min(self.max_requests, len(isect))
            
            # Ask for n rarest pieces, rarest first
            if len(isect) > 0:
                # Break symmetry
                random.shuffle(isect)
                isect.sort(key = lambda x: rarity[x])
                isect = isect[:n]
            for piece_id in isect:
                # aha! The peer has this piece! Request it.
                # which part of the piece do we need next?
                # (must get the next-needed blocks in order)
                start_block = self.pieces[piece_id]
                r = Request(self.id, peer.id, piece_id, start_block)
                requests.append(r)

        return requests

    def uploads(self, requests, peers, history):
        """
        requests -- a list of the requests for this peer for this round
        peers -- available info about all the peers
        history -- history for all previous rounds

        returns: list of Upload objects.

        In each round, this will be called after requests().
        """
        round = history.current_round()
        logging.debug("%s again.  It's round %d." % (
            self.id, round))
        # One could look at other stuff in the history too here.
        # For example, history.downloads[round-1] (if round != 0, of course)
        # has a list of Download objects for each Download to this peer in
        # the previous round.
        
        if len(requests) == 0:
            logging.debug("No one wants my pieces!")
            chosen = []
            bws = []
        else:
            logging.debug("Still here: uploading to a random peer")
            # change my internal state for no reason
            self.dummy_state["cake"] = "pie"

            other_peers = [peer.id for peer in peers]
            other_peers = list(filter(lambda x: "Seed" not in x and x != self.id, other_peers))

            # List of peers who have made requests
            requesting_peers = [request.requester_id for request in requests]

            # Optimistic unchoking. Currently this starts from round 1 since there are no requests in round zero - should we change this?
            if (round % 3) == 0:
                # Every 3 rounds, select new peer to optimistically unchoke (prioritize peer that has made a request)
                if len(requesting_peers) > 0:
                    self.optimistic_unchoke = random.choice(requesting_peers)
                else:
                    self.optimistic_unchoke = random.choice(other_peers)

            chosen = []
            chosen.append(self.optimistic_unchoke)

            # Remove optimistically unchoked peer from further consideration
            requesting_peers = list(filter(lambda x: x != self.optimistic_unchoke, requesting_peers))

            # Every round except the first, select 3 requesting peers with highest download rate in the last 2 rounds
            if round >0:

                download_rates = defaultdict(int)
                for download in history.downloads[-min(2, round)]:
                    if download.from_id in requesting_peers:
                        download_rates[download.from_id] += download.blocks
                
                # Break symmetry
                l = list(download_rates.items())
                random.shuffle(l)
                download_rates = dict(l)

                n = min(3, len(download_rates))
                chosen += sorted(download_rates, key=download_rates.get, reverse=True)[:n]                    

            # Evenly "split" my upload bandwidth among chosen requesters
            bws = even_split(self.up_bw, len(chosen))

        # create actual uploads out of the list of peer ids and bandwidths
        uploads = [Upload(self.id, peer_id, bw)
                   for (peer_id, bw) in zip(chosen, bws)]
        return uploads
