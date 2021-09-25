#!/usr/bin/python

import random
import logging
import math

from messages import Upload, Request
from util import even_split
from peer import Peer
from collections import defaultdict

class TodoketePropShare(Peer):
    def post_init(self):
        print(("post_init(): %s here!" % self.id))
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
        current_round = history.current_round()
       
        if len(requests) == 0:
            logging.debug("No one wants my pieces!")
            chosen = []
            bws = []
        else:
            chosen = []

            # List of peers who have made requests
            requesting_peers = [request.requester_id for request in requests]

            # Every round except the first, determine which requesting peers uploaded to us
            if current_round > 0:
                download_rates = defaultdict(int)
                for download in history.downloads[-1]:
                    if download.from_id in requesting_peers:
                        download_rates[download.from_id] += download.blocks

                chosen = sorted(download_rates, key = download_rates.get)
                props = [download_rates[id] for id in chosen]

            # If no requesting peers uploaded to us, split bandwidth evenly between requesting peers
            if len(chosen) == 0:
                chosen = requesting_peers
                bws = even_split(self.up_bw, len(chosen))

            else:
                requesting_peers = list(filter(lambda x: x not in props, requesting_peers))

                # Determine if a requesting peer exists that didn't upload to us last round    
                optimistic_unchoke = len(requesting_peers) > 0

                # Split upload bandwidth proportionally, accounting for optimistic unchoking.
                bw_remaining = self.up_bw
                if optimistic_unchoke:
                    optimistic_unchoke_bw = math.floor(self.up_bw * 0.1)
                    bw_remaining -= optimistic_unchoke_bw
                
                # Algorithm for allocating bandwidth, rounding to integers to ensure all bandwidth is being allocated
                t = sum(props)
                bws = []
                for i in range(len(props)):
                    bws.append(round(props[i] / t * bw_remaining))
                    t -= props[i]
                    bw_remaining -= bws[i]
                
                if optimistic_unchoke:
                    chosen += random.choice(requesting_peers)
                    bws.append(optimistic_unchoke_bw)

        # create actual uploads out of the list of peer ids and bandwidths
        uploads = [Upload(self.id, peer_id, bw)
                   for (peer_id, bw) in zip(chosen, bws)]
        return uploads
