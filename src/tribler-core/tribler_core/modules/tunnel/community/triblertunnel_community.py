import hashlib
import math
import sys
import time
from asyncio import Future, TimeoutError as AsyncTimeoutError, open_connection, sleep
from binascii import unhexlify
from collections import Counter
from distutils.version import LooseVersion

from anydex.wallet.bandwidth_block import TriblerBandwidthBlock

import async_timeout

from ipv8.attestation.trustchain.block import EMPTY_PK
from ipv8.messaging.anonymization.caches import CreateRequestCache
from ipv8.messaging.anonymization.community import message_to_payload, tc_lazy_wrapper_unsigned
from ipv8.messaging.anonymization.hidden_services import HiddenTunnelCommunity
from ipv8.messaging.anonymization.payload import (
    EstablishIntroPayload,
    NO_CRYPTO_PACKETS,
    decode_address,
    encode_address,
)
from ipv8.messaging.anonymization.tunnel import (
    CIRCUIT_STATE_CLOSING,
    CIRCUIT_STATE_READY,
    CIRCUIT_TYPE_DATA,
    CIRCUIT_TYPE_IP_SEEDER,
    CIRCUIT_TYPE_RP_DOWNLOADER,
    CIRCUIT_TYPE_RP_SEEDER,
    EXIT_NODE,
    PEER_FLAG_EXIT_ANY,
    RelayRoute,
)
from ipv8.peer import Peer
from ipv8.peerdiscovery.network import Network
from ipv8.taskmanager import task

import libtorrent as lt

from tribler_common.simpledefs import DLSTATUS_DOWNLOADING, DLSTATUS_METADATA, DLSTATUS_SEEDING, DLSTATUS_STOPPED, NTFY

from tribler_core.modules.tunnel.community.caches import BalanceRequestCache, HTTPRequestCache
from tribler_core.modules.tunnel.community.discovery import GoldenRatioStrategy
from tribler_core.modules.tunnel.community.dispatcher import TunnelDispatcher
from tribler_core.modules.tunnel.community.payload import (
    BalanceRequestPayload,
    BalanceResponsePayload,
    HTTPRequestPayload,
    HTTPResponsePayload,
    PayoutPayload,
)
from tribler_core.modules.tunnel.socks5.server import Socks5Server
from tribler_core.utilities import path_util
from tribler_core.utilities.unicode import hexlify
from tribler_core.utilities.utilities import succeed

DESTROY_REASON_BALANCE = 65535

PEER_FLAG_EXIT_HTTP = 32768

MAX_HTTP_PACKET_SIZE = 1400


class TriblerTunnelCommunity(HiddenTunnelCommunity):
    """
    This community is built upon the anonymous messaging layer in IPv8.
    It adds support for libtorrent anonymous downloads and bandwidth token payout when closing circuits.
    """

    master_peer = ("4c69624e61434c504b3ad051d49fbceef0172b68f709818ab00ec1a98461ac4d76dbfe5ec69f3d5eeb34ead4fc5"
                   "f280bddd4e3381a2b70e70f78a4f783819dbe2146fce70efc0097d600")
    master_peer = Peer(unhexlify(master_peer))

    def __init__(self, *args, **kwargs):
        self.tribler_session = kwargs.pop('tribler_session', None)
        num_competing_slots = kwargs.pop('competing_slots', 15)
        num_random_slots = kwargs.pop('random_slots', 5)
        self.bandwidth_wallet = kwargs.pop('bandwidth_wallet', None)
        socks_listen_ports = kwargs.pop('socks_listen_ports', None)
        state_path = self.tribler_session.config.get_state_dir() if self.tribler_session else path_util.Path()
        self.exitnode_cache = kwargs.pop('exitnode_cache', state_path / 'exitnode_cache.dat')
        super(TriblerTunnelCommunity, self).__init__(*args, **kwargs)
        self._use_main_thread = True

        if self.tribler_session:
            if self.tribler_session.config.get_tunnel_community_exitnode_enabled():
                self.settings.peer_flags.add(PEER_FLAG_EXIT_ANY)
                self.settings.peer_flags.add(PEER_FLAG_EXIT_HTTP)

            if not socks_listen_ports:
                socks_listen_ports = self.tribler_session.config.get_tunnel_community_socks5_listen_ports()
        elif socks_listen_ports is None:
            socks_listen_ports = range(1080, 1085)

        self.bittorrent_peers = {}
        self.dispatcher = TunnelDispatcher(self)
        self.download_states = {}
        self.competing_slots = [(0, None)] * num_competing_slots  # 1st tuple item = token balance, 2nd = circuit id
        self.random_slots = [None] * num_random_slots
        self.reject_callback = None  # This callback is invoked with a tuple (time, balance) when we reject a circuit
        self.last_forced_announce = {}

        # Start the SOCKS5 servers
        self.socks_servers = []
        for port in socks_listen_ports:
            socks_server = Socks5Server(port, self.dispatcher)
            self.register_task('start_socks_%d' % port, socks_server.start)
            self.socks_servers.append(socks_server)

        self.dispatcher.set_socks_servers(self.socks_servers)

        self.decode_map.update({
            chr(23): self.on_payout_block,
        })

        self.decode_map_private.update({
            chr(24): self.on_balance_request_cell,
            chr(25): self.on_relay_balance_request_cell,
            chr(26): self.on_balance_response_cell,
            chr(27): self.on_relay_balance_response_cell,
            chr(28): self.on_http_request,
            chr(29): self.on_http_response
        })

        message_to_payload["balance-request"] = (24, BalanceRequestPayload)
        message_to_payload["relay-balance-request"] = (25, BalanceRequestPayload)
        message_to_payload["balance-response"] = (26, BalanceResponsePayload)
        message_to_payload["relay-balance-response"] = (27, BalanceResponsePayload)
        message_to_payload["http-request"] = (28, HTTPRequestPayload)
        message_to_payload["http-response"] = (29, HTTPResponsePayload)

        NO_CRYPTO_PACKETS.extend([24, 26])

        if self.exitnode_cache:
            self.restore_exitnodes_from_disk()

    async def wait_for_socks_servers(self):
        # Wait for the socks server to be ready. Otherwise, hidden services downloads may fail.
        while any([name.startswith('start_socks_') for name in self._pending_tasks.keys()]):
            await sleep(.05)

    def get_available_strategies(self):
        return super(TriblerTunnelCommunity, self).get_available_strategies().update({'GoldenRatioStrategy':
                                                                                          GoldenRatioStrategy})

    def cache_exitnodes_to_disk(self):
        """
        Wite a copy of the exit_candidates to the file self.exitnode_cache.

        :returns: None
        """
        exit_nodes = Network()
        for peer in self.get_candidates(PEER_FLAG_EXIT_ANY):
            exit_nodes.add_verified_peer(peer)
        self.logger.debug('Writing exit nodes to cache: %s', self.exitnode_cache)
        with open(self.exitnode_cache, 'wb') as cache:
            cache.write(exit_nodes.snapshot())

    def restore_exitnodes_from_disk(self):
        """
        Send introduction requests to peers stored in the file self.exitnode_cache.

        :returns: None
        """
        if self.exitnode_cache.is_file():
            self.logger.debug('Loading exit nodes from cache: %s', self.exitnode_cache)
            exit_nodes = Network()
            with self.exitnode_cache.open('rb') as cache:
                exit_nodes.load_snapshot(cache.read())
            for exit_node in exit_nodes.get_walkable_addresses():
                self.endpoint.send(exit_node, self.create_introduction_request(exit_node))
        else:
            self.logger.error('Could not retrieve backup exitnode cache, file does not exist!')

    def on_token_balance(self, circuit_id, balance):
        """
        We received the token balance of a circuit initiator. Check whether we can allocate a slot to this user.
        """
        if not self.request_cache.has("balance-request", circuit_id):
            self.logger.warning("Received token balance without associated request cache!")
            return

        cache = self.request_cache.pop("balance-request", circuit_id)

        lowest_balance = sys.maxsize
        lowest_index = -1
        for ind, tup in enumerate(self.competing_slots):
            if not tup[1]:
                # The slot is empty, take it
                self.competing_slots[ind] = (balance, circuit_id)
                cache.balance_future.set_result(True)
                return

            if tup[0] < lowest_balance:
                lowest_balance = tup[0]
                lowest_index = ind

        if balance > lowest_balance:
            # We kick this user out
            old_circuit_id = self.competing_slots[lowest_index][1]
            self.logger.info("Kicked out circuit %s (balance: %s) in favor of %s (balance: %s)",
                             old_circuit_id, lowest_balance, circuit_id, balance)
            self.competing_slots[lowest_index] = (balance, circuit_id)

            self.remove_relay(old_circuit_id, destroy=DESTROY_REASON_BALANCE)
            self.remove_exit_socket(old_circuit_id, destroy=DESTROY_REASON_BALANCE)

            cache.balance_future.set_result(True)
        else:
            # We can't compete with the balances in the existing slots
            if self.reject_callback:
                self.reject_callback(time.time(), balance)
            cache.balance_future.set_result(False)

    def should_join_circuit(self, create_payload, previous_node_address):
        """
        Check whether we should join a circuit. Returns a future that fires with a boolean.
        """
        if self.settings.max_joined_circuits <= len(self.relay_from_to) + len(self.exit_sockets):
            self.logger.warning("too many relays (%d)", (len(self.relay_from_to) + len(self.exit_sockets)))
            return succeed(False)

        # Check whether we have a random open slot, if so, allocate this to this request.
        circuit_id = create_payload.circuit_id
        for index, slot in enumerate(self.random_slots):
            if not slot:
                self.random_slots[index] = circuit_id
                return succeed(True)

        # No random slots but this user might be allocated a competing slot.
        # Next, we request the token balance of the circuit initiator.
        balance_future = Future()
        self.request_cache.add(BalanceRequestCache(self, circuit_id, balance_future))

        # Temporarily add these values, otherwise we are unable to communicate with the previous hop.
        self.directions[circuit_id] = EXIT_NODE
        shared_secret, _, _ = self.crypto.generate_diffie_shared_secret(create_payload.key)
        self.relay_session_keys[circuit_id] = self.crypto.generate_session_keys(shared_secret)

        self.send_cell(Peer(create_payload.node_public_key, previous_node_address), "balance-request",
                       BalanceRequestPayload(circuit_id))

        self.directions.pop(circuit_id, None)
        self.relay_session_keys.pop(circuit_id, None)

        return balance_future

    async def on_payout_block(self, source_address, data):
        if not self.bandwidth_wallet:
            self.logger.warning("Got payout while not having a TrustChain community running!")
            return

        payload = self._ez_unpack_noauth(PayoutPayload, data, global_time=False)
        peer = Peer(payload.public_key, source_address)
        block = TriblerBandwidthBlock.from_payload(payload, self.serializer)

        try:
            blocks = await self.bandwidth_wallet.trustchain.process_half_block(block, peer)
        except:
            return

        # Send the next payout
        if blocks and payload.circuit_id in self.relay_from_to and block.transaction[b'down'] > payload.base_amount:
            relay = self.relay_from_to[payload.circuit_id]
            self._logger.info("Sending next payout to peer %s", relay.peer)
            self.do_payout(relay.peer, relay.circuit_id, block.transaction[b'down'] - payload.base_amount * 2,
                           payload.base_amount)

        # Check whether the block has been added to the database and has been verified
        if not self.bandwidth_wallet.trustchain.persistence.contains(block):
            self.logger.warning("Not proceeding with payout - received payout block is not valid")
            return

    def on_balance_request_cell(self, source_address, data, _):
        payload = self._ez_unpack_noauth(BalanceRequestPayload, data, global_time=False)

        if self.request_cache.has("create", payload.circuit_id):
            request = self.request_cache.get("create", payload.circuit_id)
            forwarding_relay = RelayRoute(request.from_circuit_id, request.peer)
            self.send_cell(forwarding_relay.peer, "relay-balance-request",
                           BalanceRequestPayload(forwarding_relay.circuit_id))
        elif self.request_cache.has("retry", payload.circuit_id):
            self.on_balance_request(payload)
        else:
            self.logger.warning("Circuit creation cache for id %s not found!", payload.circuit_id)

    def on_relay_balance_request_cell(self, source_address, data, _):
        payload = self._ez_unpack_noauth(BalanceRequestPayload, data, global_time=False)
        self.on_balance_request(payload)

    def on_balance_request(self, payload):
        """
        We received a balance request from a relay or exit node. Respond with the latest block in our chain.
        """
        if not self.bandwidth_wallet:
            self.logger.warning("Bandwidth wallet is not available, not sending balance response!")
            return

        # Get the latest block
        latest_block = self.bandwidth_wallet.trustchain.persistence.get_latest(self.my_peer.public_key.key_to_bin(),
                                                                               block_type=b'tribler_bandwidth')
        if not latest_block:
            latest_block = TriblerBandwidthBlock()
        latest_block.public_key = EMPTY_PK  # We hide the public key

        # We either send the response directly or relay the response to the last verified hop
        circuit = self.circuits[payload.circuit_id]
        if not circuit.hops:
            self.increase_bytes_sent(circuit, self.send_cell(circuit.peer,
                                                             "balance-response",
                                                             BalanceResponsePayload.from_half_block(
                                                                 latest_block, circuit.circuit_id)))
        else:
            self.increase_bytes_sent(circuit, self.send_cell(circuit.peer,
                                                             "relay-balance-response",
                                                             BalanceResponsePayload.from_half_block(
                                                                 latest_block, circuit.circuit_id)))

    def on_balance_response_cell(self, source_address, data, _):
        payload = self._ez_unpack_noauth(BalanceResponsePayload, data, global_time=False)
        block = TriblerBandwidthBlock.from_payload(payload, self.serializer)
        if not block.transaction:
            self.on_token_balance(payload.circuit_id, 0)
        else:
            self.on_token_balance(payload.circuit_id,
                                  block.transaction[b"total_up"] - block.transaction[b"total_down"])

    def on_relay_balance_response_cell(self, source_address, data, _):
        payload = self._ez_unpack_noauth(BalanceResponsePayload, data, global_time=False)
        block = TriblerBandwidthBlock.from_payload(payload, self.serializer)

        # At this point, we don't have the circuit ID of the follow-up hop. We have to iterate over the items in the
        # request cache and find the link to the next hop.
        for cache in self.request_cache._identifiers.values():
            if isinstance(cache, CreateRequestCache) and cache.from_circuit_id == payload.circuit_id:
                self.send_cell(cache.to_peer,
                               "balance-response",
                               BalanceResponsePayload.from_half_block(block, cache.to_circuit_id))

    def readd_bittorrent_peers(self):
        for torrent, peers in list(self.bittorrent_peers.items()):
            infohash = hexlify(torrent.tdef.get_infohash())
            for peer in peers:
                self.logger.info("Re-adding peer %s to torrent %s", peer, infohash)
                torrent.add_peer(peer)
            del self.bittorrent_peers[torrent]

    def update_torrent(self, peers, download):
        if not download.handle or not download.handle.is_valid():
            return

        peers = peers.intersection({pi.ip for pi in download.handle.get_peer_info()})
        if peers:
            if download not in self.bittorrent_peers:
                self.bittorrent_peers[download] = peers
            else:
                self.bittorrent_peers[download] = peers | self.bittorrent_peers[download]

            # If there are active circuits, add peers immediately. Otherwise postpone.
            if self.find_circuits():
                self.readd_bittorrent_peers()

    def do_payout(self, peer, circuit_id, amount, base_amount):
        """
        Perform a payout to a specific peer.
        :param peer: The peer to perform the payout to, usually the next node in the circuit.
        :param circuit_id: The circuit id of the payout, used by the subsequent node.
        :param amount: The amount to put in the transaction, multiplier of base_amount.
        :param base_amount: The base amount for the payouts.
        """
        self.logger.info("Sending payout of %d (base: %d) to %s (cid: %s)", amount, base_amount, peer, circuit_id)

        block = TriblerBandwidthBlock.create(
            b'tribler_bandwidth',
            {b'up': 0, b'down': amount},
            self.bandwidth_wallet.trustchain.persistence,
            self.my_peer.public_key.key_to_bin(),
            link_pk=peer.public_key.key_to_bin())
        block.sign(self.my_peer.key)
        self.bandwidth_wallet.trustchain.persistence.add_block(block)

        payload = PayoutPayload.from_half_block(block, circuit_id, base_amount).to_pack_list()
        packet = self._ez_pack(self._prefix, 23, [payload], False)
        self.send_packet(peer, packet)

    def clean_from_slots(self, circuit_id):
        """
        Clean a specific circuit from the allocated slots.
        """
        for ind, slot in enumerate(self.random_slots):
            if slot == circuit_id:
                self.random_slots[ind] = None

        for ind, tup in enumerate(self.competing_slots):
            if tup[1] == circuit_id:
                self.competing_slots[ind] = (0, None)

    def remove_circuit(self, circuit_id, additional_info='', remove_now=False, destroy=False):
        if circuit_id not in self.circuits:
            self.logger.warning("Circuit %d not found when trying to remove it", circuit_id)
            return succeed(None)

        circuit = self.circuits[circuit_id]

        # Send the notification
        if self.tribler_session:
            self.tribler_session.notifier.notify(NTFY.TUNNEL_REMOVE, circuit, additional_info)

        # Ignore circuits that are closing so we do not payout again if we receive a destroy message.
        if circuit.state != CIRCUIT_STATE_CLOSING and circuit.bytes_down >= 1024 * 1024 and self.bandwidth_wallet:

            # We should perform a payout of the removed circuit.
            if circuit.ctype == CIRCUIT_TYPE_RP_DOWNLOADER:
                # We remove an e2e circuit as downloader. We pay the subsequent nodes in the downloader part of the e2e
                # circuit. In addition, we pay for one hop seeder anonymity since we don't know the circuit length at
                # the seeder side.
                self.do_payout(circuit.peer, circuit_id, circuit.bytes_down * ((circuit.goal_hops * 2) + 1),
                               circuit.bytes_down)

            if circuit.ctype == CIRCUIT_TYPE_DATA:
                # We remove a regular data circuit as downloader. Pay the relay nodes and the exit nodes.
                self.do_payout(circuit.peer, circuit_id, circuit.bytes_down * (circuit.goal_hops * 2 - 1),
                               circuit.bytes_down)

        affected_peers = self.dispatcher.circuit_dead(circuit)

        # Make sure the circuit is marked as closing, otherwise we may end up reusing it
        circuit.close()

        if self.tribler_session and self.tribler_session.config.get_libtorrent_enabled():
            for download in self.tribler_session.dlmgr.get_downloads():
                self.update_torrent(affected_peers, download)

        # Now we actually remove the circuit
        return super(TriblerTunnelCommunity, self).remove_circuit(circuit_id, additional_info=additional_info,
                                                                  remove_now=remove_now, destroy=destroy)

    @task
    async def remove_relay(self, circuit_id, additional_info='', remove_now=False, destroy=False,
                           got_destroy_from=None, both_sides=True):
        removed_relays = await super(TriblerTunnelCommunity, self).remove_relay(circuit_id,
                                                                                additional_info=additional_info,
                                                                                remove_now=remove_now,
                                                                                destroy=destroy,
                                                                                got_destroy_from=got_destroy_from,
                                                                                both_sides=both_sides)

        self.clean_from_slots(circuit_id)

        if self.tribler_session:
            for removed_relay in removed_relays:
                self.tribler_session.notifier.notify(NTFY.TUNNEL_REMOVE, removed_relay, additional_info)

    def remove_exit_socket(self, circuit_id, additional_info='', remove_now=False, destroy=False):
        if circuit_id in self.exit_sockets and self.tribler_session:
            exit_socket = self.exit_sockets[circuit_id]
            self.tribler_session.notifier.notify(NTFY.TUNNEL_REMOVE, exit_socket, additional_info)

        self.clean_from_slots(circuit_id)

        return super(TriblerTunnelCommunity, self).remove_exit_socket(circuit_id, additional_info=additional_info,
                                                                      remove_now=remove_now, destroy=destroy)

    def _ours_on_created_extended(self, circuit, payload):
        super(TriblerTunnelCommunity, self)._ours_on_created_extended(circuit, payload)

        if circuit.state == CIRCUIT_STATE_READY:
            # Re-add BitTorrent peers, if needed.
            self.readd_bittorrent_peers()

    def on_raw_data(self, circuit, origin, data):
        anon_seed = circuit.ctype == CIRCUIT_TYPE_RP_SEEDER
        self.dispatcher.on_incoming_from_tunnel(self, circuit, origin, data, anon_seed)

    def monitor_downloads(self, dslist):
        # Monitor downloads with anonymous flag set, and build rendezvous/introduction points when needed.
        new_states = {}
        hops = {}
        active_downloads_per_hop = {}

        for ds in dslist:
            download = ds.get_download()
            hop_count = download.config.get_hops()
            if hop_count > 0:
                # Convert the real infohash to the infohash used for looking up introduction points
                real_info_hash = download.get_def().get_infohash()
                info_hash = self.get_lookup_info_hash(real_info_hash)
                hops[info_hash] = hop_count
                new_states[info_hash] = ds.get_status()

                if download.get_state().get_status() in [DLSTATUS_DOWNLOADING, DLSTATUS_SEEDING, DLSTATUS_METADATA]:
                    active_downloads_per_hop[hop_count] = active_downloads_per_hop.get(hop_count, 0) + 1

                    # Ugly work-around for the libtorrent DHT not making any requests
                    # after a period of having no circuits
                    if self.last_forced_announce.get(info_hash, 0) + 60 <= time.time() \
                            and self.find_circuits(hops=hop_count) \
                            and not ds.get_peerlist():
                        download.force_dht_announce()
                        self.last_forced_announce[info_hash] = time.time()

        # Request 1 circuit per download while ensuring that the total number of circuits requested per hop count
        # stays within min_circuits and max_circuits.
        self.circuits_needed = {hop_count: min(max(download_count, self.settings.min_circuits),
                                               self.settings.max_circuits)
                                for hop_count, download_count in active_downloads_per_hop.items()}

        ip_counter = Counter([c.info_hash for c in list(self.circuits.values()) if c.ctype == CIRCUIT_TYPE_IP_SEEDER])
        for info_hash in set(list(new_states) + list(self.download_states)):
            new_state = new_states.get(info_hash, None)
            old_state = self.download_states.get(info_hash, None)
            state_changed = new_state != old_state

            # Join/leave hidden swarm as needed.
            if state_changed and new_state in [DLSTATUS_DOWNLOADING, DLSTATUS_SEEDING]:
                self.join_swarm(info_hash, hops[info_hash], seeding=new_state == DLSTATUS_SEEDING,
                                callback=lambda addr, ih=info_hash: self.on_e2e_finished(addr, ih))
            elif state_changed and new_state in [DLSTATUS_STOPPED, None]:
                self.leave_swarm(info_hash)

            # Ensure we have enough introduction points for this infohash. Currently, we only create 1.
            if new_state == DLSTATUS_SEEDING:
                for _ in range(1 - ip_counter.get(info_hash, 0)):
                    self.logger.info('Create introducing circuit for %s', hexlify(info_hash))
                    self.create_introduction_point(info_hash)

        self.download_states = new_states

    def on_e2e_finished(self, address, info_hash):
        dl = self.get_download(info_hash)
        if dl:
            dl.add_peer(address)
        else:
            self.logger.error('Could not find download for adding hidden services peer %s:%d!', *address)

    def on_establish_intro(self, source_address, data, circuit_id):
        payload = self._ez_unpack_noauth(EstablishIntroPayload, data, global_time=False)
        exists_before = payload.public_key in self.intro_point_for
        super(TriblerTunnelCommunity, self).on_establish_intro(source_address, data, circuit_id)
        # Check if an introduction point was just added
        if not exists_before and payload.public_key in self.intro_point_for:
            self.clean_from_slots(circuit_id)

    def on_rendezvous_established(self, source_address, data, circuit_id):
        super(TriblerTunnelCommunity, self).on_rendezvous_established(source_address, data, circuit_id)

        circuit = self.circuits.get(circuit_id)
        if circuit:
            self.update_ip_filter(circuit.info_hash)

    def update_ip_filter(self, info_hash):
        download = self.get_download(info_hash)
        lt_session = self.tribler_session.dlmgr.get_session(download.config.get_hops())
        ip_addresses = [self.circuit_id_to_ip(c.circuit_id)
                        for c in self.find_circuits(ctype=CIRCUIT_TYPE_RP_SEEDER)] + ['1.1.1.1']
        self.tribler_session.dlmgr.update_ip_filter(lt_session, ip_addresses)

    def get_download(self, lookup_info_hash):
        if not self.tribler_session:
            return None

        for download in self.tribler_session.dlmgr.get_downloads():
            if lookup_info_hash == self.get_lookup_info_hash(download.get_def().get_infohash()):
                return download

    @task
    async def create_introduction_point(self, info_hash, required_ip=None):
        download = self.get_download(info_hash)
        if download:
            # We now have to associate the SOCKS5 UDP connection with the libtorrent listen port ourselves.
            # The reason for this is that libtorrent does not include the source IP/port in an SOCKS5 ASSOCIATE message.
            # In libtorrent < 1.2.0, we could do so by simply adding an (invalid) peer to the download to enforce
            # an outgoing message through the SOCKS5 port.
            # This does not seem to work anymore in libtorrent 1.2.0 (and probably higher) so we manually associate
            # the connection and the libtorrent listen port.
            if LooseVersion(self.tribler_session.dlmgr.get_libtorrent_version()) < LooseVersion("1.2.0"):
                download.add_peer(('1.1.1.1', 1024))
            else:
                hops = download.config.get_hops()
                lt_listen_port = self.tribler_session.dlmgr.get_session(hops).listen_port()
                for session in self.socks_servers[hops - 1].sessions:
                    if session.get_udp_socket():
                        session.get_udp_socket().remote_udp_address = ("127.0.0.1", lt_listen_port)
        await super(TriblerTunnelCommunity, self).create_introduction_point(info_hash, required_ip=required_ip)

    async def unload(self):
        if self.bandwidth_wallet:
            await self.bandwidth_wallet.shutdown_task_manager()
        for socks_server in self.socks_servers:
            await socks_server.stop()

        if self.exitnode_cache:
            self.cache_exitnodes_to_disk()

        await super(TriblerTunnelCommunity, self).unload()

    def get_lookup_info_hash(self, info_hash):
        return hashlib.sha1(b'tribler anonymous download' + hexlify(info_hash).encode('utf-8')).digest()

    @tc_lazy_wrapper_unsigned(HTTPRequestPayload)
    async def on_http_request(self, source_address, payload, circuit_id):
        if circuit_id not in self.exit_sockets:
            self.logger.warning("Received unexpected http-request")
            return
        if len([cache for cache in self.request_cache._identifiers.values()
                if isinstance(cache, HTTPRequestCache) and cache.circuit_id == circuit_id]) > 5:
            self.logger.warning("Too many HTTP requests coming from circuit %s")
            return

        self.logger.debug("Got http-request from %s", source_address)
        target_address = decode_address(payload.target)

        writer = None
        try:
            with async_timeout.timeout(3):
                self.logger.debug("Opening TCP connection to %s", target_address)
                reader, writer = await open_connection(*target_address)
                writer.write(payload.request)
                response = b''
                while True:
                    line = await reader.readline()
                    response += line
                    if not line.strip():
                        # Read HTTP response body (1MB max)
                        response += await reader.read(1024**2)
                        break
        except OSError:
            self.logger.warning('Tunnel HTTP request failed')
            return
        except AsyncTimeoutError:
            self.logger.warning('Tunnel HTTP request timed out')
            return
        finally:
            if writer:
                writer.close()

        # Note that, depending on the libtorrent version, bdecode does not always raise
        # an exception, sometimes it returns None instead.
        try:
            response_decoded = lt.bdecode(response.split(b'\r\n\r\n')[1])
        except (IndexError, RuntimeError):
            response_decoded = None

        if response_decoded is None:
            self.logger.warning('Tunnel HTTP request not allowed')
            return

        num_cells = math.ceil(len(response) / MAX_HTTP_PACKET_SIZE)
        for i in range(num_cells):
            self.send_cell(source_address, "http-response",
                           HTTPResponsePayload(circuit_id, payload.identifier, i, num_cells,
                                               response[i*MAX_HTTP_PACKET_SIZE:(i+1)*MAX_HTTP_PACKET_SIZE]))

    @tc_lazy_wrapper_unsigned(HTTPResponsePayload)
    def on_http_response(self, source_address, payload, circuit_id):
        if not self.request_cache.has("http-request", payload.identifier):
            self.logger.warning("Received unexpected http-response")
            return
        cache = self.request_cache.get("http-request", payload.identifier)
        if cache.circuit_id != circuit_id:
            self.logger.warning("Received http-response from wrong circuit")
            return

        self.logger.debug("Got http-response from %s", source_address)
        if cache.add_response(payload):
            self.request_cache.pop("http-request", payload.identifier)

    async def perform_http_request(self, destination, request, hops=1):
        # We need a circuit that supports HTTP requests, meaning that the circuit will have to end
        # with a node that has the PEER_FLAG_EXIT_HTTP flag set.
        allowed_peers = self.get_candidates(PEER_FLAG_EXIT_HTTP)
        allowed_pks = [p.key.key_to_bin() for p in allowed_peers]
        circuits = [c for c in self.circuits.values() if c.state == CIRCUIT_STATE_READY
                    and c.hops[-1].public_key in allowed_pks
                    and c.goal_hops == hops]

        if circuits:
            circuit = circuits[0]
        elif allowed_peers:
            circuit = self.create_circuit(hops, required_exit=allowed_peers[0])
            if not circuit or not await circuit.ready:
                raise RuntimeError('No HTTP circuit could be created')
        else:
            raise RuntimeError('No HTTP exits available')

        cache = self.request_cache.add(HTTPRequestCache(self, circuit.circuit_id))
        self.increase_bytes_sent(circuit, self.send_cell(circuit.peer, "http-request",
                                                         HTTPRequestPayload(circuit.circuit_id,
                                                                            cache.number,
                                                                            encode_address(*destination),
                                                                            request)))
        return await cache.response_future


class TriblerTunnelTestnetCommunity(TriblerTunnelCommunity):
    """
    This community defines a testnet for the anonymous tunnels.
    """

    master_peer = ("4c69624e61434c504b3a7773497078402b39b61b4d26851dad4a8d956243558ff387239a80429c362f52df70e1e"
                   "8a97182311d7fb9c13fb80fae616bc82c691c7f03e69560da8c7295d8")
    master_peer = Peer(unhexlify(master_peer))
