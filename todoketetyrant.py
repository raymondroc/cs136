#!/usr/bin/python

import random
import logging

from messages import Upload, Request
from util import even_split
from peer import Peer
from collections import defaultdict

class TodoketeTyrant(Peer):
    
    def post_init(self):
        print(("post_init(): %s here!" % self.id))
        self.alpha = 0.20
        self.r = 3
        self.gamma = 0.10
        self.dinit = 3
        self.uinit = 3
    
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
        random.shuffle(peers)
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
            
            # List of peers who have made requests
            requesting_peers = [request.requester_id for request in requests]

            # Create download rate estimates for peers
            dlr_ests = defaultdict(int)
            for peer in peers:
                dlr_ests[peer] = self.dinit
                for round_dls in reversed(history.downloads[peer]):
                    if round_dls:
                        dlr_ests[peer] = len(round_dls)
                        break
                    
            # Create upload rate estimates for peers
            ulr_ests = defaultdict(int)
            for peer in requesting_peers:
                ulr = self.uinit
                for idx, round_uls in enumerate(history.uploads[peer]):
                    # If we unchoked peer, update
                    if round_uls: 
                        # If peer didn't unchoke us, increase ulr
                        if not history.downloads[peer][idx]:
                            ulr = (1 + self.alpha) * ulr
                        # If peer unchoked us for last r rounds, decrease ulr
                        elif round >= self.r:
                            last_r = True
                            for prev_round in reversed(history.downloads[peer][idx-self.r+1:idx]):
                                if not prev_round:
                                    last_r = False
                            if last_r:
                                ulr = (1 - self.gamma) * ulr
                ulr_ests[peer] = ulr

            # Sort requesting peers by decreasing dlr/ulr
            requesting_peers = sorted(requesting_peers, key=lambda peer: dlr_ests[peer]/ulr_ests[peer], reverse=True)
            
            # Add upload slots until cap is reached
            chosen = []
            bws = []
            bw_remaining = self.up_bw
            for peer in requesting_peers:
                bw_remaining -= ulr_ests[peer]
                if bw_remaining < 0:
                    break
                chosen.append(peer)
                bws.append(ulr_ests[peer])

        # Create actual uploads out of the list of peer ids and bandwidths
        uploads = [Upload(self.id, peer_id, bw)
                   for (peer_id, bw) in zip(chosen, bws)]
        return uploads
