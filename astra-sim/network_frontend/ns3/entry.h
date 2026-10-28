#undef PGO_TRAINING
#define PATH_TO_PGO_CONFIG "path_to_pgo_config"

#include "common.h"
#include "ns3/applications-module.h"
#include "ns3/core-module.h"
#include "ns3/error-model.h"
#include "ns3/global-route-manager.h"
#include "ns3/internet-module.h"
#include "ns3/ipv4-static-routing-helper.h"
#include "ns3/packet.h"
#include "ns3/point-to-point-helper.h"
#include "ns3/qbb-helper.h"
#include <fstream>
#include <iostream>
#include <queue>
#include <ns3/hash.h>
#include <ns3/rdma-client-helper.h>
#include <ns3/rdma-client.h>
#include <ns3/rdma-driver.h>
#include <ns3/rdma.h>
#include <ns3/rdma-cc-trace.h>
#include <ns3/sim-setting.h>
#include <ns3/switch-node.h>
#include <time.h>
#include <unordered_map>

using namespace ns3;
using namespace std;

/*
 * This file defines the interaction between the System layer and the NS3
 * simulator (Network layer). The system layer issues send/receive events, and
 * waits until the ns3 simulates the conclusion of these events to issue the
 * next collective communication. When ns3 simulates the conclusion of an event,
 * it will call qp_finish to lookup the maps in this file and call the callback
 * handlers. Refer to below comments for further detail.
 *
 * Two transport connection modes exist (TRANSPORT_CONNECTION_MODE in the
 * network config, default LEGACY):
 *
 *   LEGACY      every sim_send allocates a new source port and a new queue
 *               pair; the queue pair completes with the message. This is the
 *               original behaviour and is kept verbatim below (send_flow,
 *               qp_finish, notify_*).
 *   PERSISTENT  one queue pair per connection key (src, dst, priority,
 *               channel) is created on first use and reused for every later
 *               logical message on that key. Messages are posted in order as
 *               work requests; each keeps its own completion callback. The
 *               queue pair, its source port, its congestion-control state and
 *               its timers live until the workload finishes.
 */

// MsgEvent represents a single send or receive event, issued by the system
// layer. The system layer will wait for the ns3 backend to simulate the event
// finishing (i.e. node 0 finishes sending message, or node 1 finishes receiving
// the message) The callback handler 'msg_handler' signals the System layer that
// the event has finished in ns3.
class MsgEvent {
public:
  int src_id;
  int dst_id;
  int type;
  // Indicates the number of bytes remaining to be sent or received.
  // Initialized with the original size of the message, and
  // incremented/decremented depending on how many bytes were sent/received.
  // Eventually, this value will reach 0 when the event has completed.
  int remaining_msg_bytes;
  void *fun_arg;
  void (*msg_handler)(void *fun_arg);

  MsgEvent(int _src_id, int _dst_id, int _type, int _remaining_msg_bytes,
           void *_fun_arg, void (*_msg_handler)(void *fun_arg))
      : src_id(_src_id), dst_id(_dst_id), type(_type),
        remaining_msg_bytes(_remaining_msg_bytes), fun_arg(_fun_arg),
        msg_handler(_msg_handler) {}

  // Default constructor to prevent compile errors. When looking up MsgEvents
  // from maps such as sim_send_waiting_hash, we should always check that a MsgEvent exists
  // for the given key. (i.e. this default constructor should not be called in
  // runtime.)
  MsgEvent()
      : src_id(0), dst_id(0), type(0), remaining_msg_bytes(0), fun_arg(nullptr),
        msg_handler(nullptr) {}

  // CallHandler will call the callback handler associated with this MsgEvent.
  void callHandler() {
    msg_handler(fun_arg);
    return;
  }
};

// MsgEventKey is a key to uniquely identify each MsgEvent.
//  - Pair <Tag, Pair <src_id, dst_id>>
typedef pair<int, pair<int, int>> MsgEventKey;

// The ns3 RdmaClient structure cannot hold the 'tag' information, which is a
// Astra-sim specific implementation. We use a mapping with the source port
// number (another unique value) to hold tag information.
//   - key: Pair <port_id, Pair <src_id, dst_id>>
//   - value: tag
// TODO: It seems we *can* obtain the tag through q->GetTag() at qp_finish.
// Verify & Simplify.
map<pair<int, pair<int, int>>, int> sender_src_port_map;

// NodeHash is used to count how many bytes were sent/received by this node.
// Refer to sim_finish().
//   - key: Pair <node_id, send/receive>. Where 'send/receive' indicates if the
//   value is for send or receive
//   - value: Number of bytes this node has sent (if send/receive is 0) and
//   received (if send/receive is 1)
map<pair<int, int>, int> node_to_bytes_sent_map;

// SentHash stores a MsgEvent for sim_send events and its callback handler.
//   - key: A pair of <MsgEventKey, port_id>. 
//          A single collective phase can be split into multiple sim_send messages, which all have the same MsgEventKey. 
//          TODO: Adding port_id as key is a hacky solution. The real solution would be to split this map, similar to sim_recv_waiting_hash and received_msg_standby_hash.
//   - value: A MsgEvent instance that indicates that Sys layer is waiting for a
//   send event to finish
map<pair<MsgEventKey, int>, MsgEvent> sim_send_waiting_hash;

// While ns3 cannot send packets before System layer calls sim_send, it
// is possible for ns3 to simulate Incoming messages before System layer calls
// sim_recv to 'reap' the messages. Therefore, we maintain two maps:
//   - sim_recv_waiting_hash holds messages where sim_recv has been called but
//   ns3 has not yet simulated the message arriving,
//   - received_msg_standby_hash holds messages which ns3 has simulated the
//   arrival, but sim_recv has not yet been called.

//   - key: A MsgEventKey instance.
//   - value: A queue of MsgEvent instances that the Sys layer is waiting for.
//   Multiple receive events may share the same key, so each callback must be
//   retained until its corresponding bytes arrive.
map<MsgEventKey, queue<MsgEvent>> sim_recv_waiting_hash;

//   - key: A MsgEventKey isntance.
//   - value: The number of bytes that ns3 has simulated completed, but the
//   System layer has not yet called sim_recv
map<MsgEventKey, int> received_msg_standby_hash;

/*********************************************************************
 * Logical-message metadata, flow/path logging and counters.
 * All of this is observational. The LEGACY data path below only gains
 * bookkeeping; it does not change what the simulator does.
 ********************************************************************/
static const uint64_t META_UNSPECIFIED = ~0ULL;
static const int RENDEZVOUS_TAG_OFFSET = 1000000000; // Sys::FrontEndSendRecvType::RENDEZVOUS

// Mirrors the optional attribution fields of AstraSim::sim_request.
struct MsgMeta {
  uint64_t collective_id = META_UNSPECIFIED;
  uint64_t operation_id = META_UNSPECIFIED;
  uint64_t phase_id = META_UNSPECIFIED;
  uint64_t chunk_id = META_UNSPECIFIED;
  uint64_t communicator_id = META_UNSPECIFIED;
  uint64_t channel_id = META_UNSPECIFIED;
};

struct LogicalMessage {
  uint64_t msg_id = 0;
  int src = 0, dst = 0;
  uint64_t bytes = 0;
  int tag = 0;
  bool handshake = false; // rendezvous 8 KiB handshake rather than payload
  MsgMeta meta;
  uint64_t conn_id = 0;   // 0 in LEGACY mode
  uint64_t qp_id = 0;
  uint16_t sport = 0, dport = 0;
  Time issue_time;         // sim_send
  Time network_start_time; // first byte sent (persistent) / QP created (legacy)
  Time completion_time;
  void *fun_arg = nullptr;
  void (*msg_handler)(void *fun_arg) = nullptr;
  std::string path;
};

// Connection key for PERSISTENT mode. Message, chunk and phase identity are
// deliberately not part of it: they identify logical messages, not transport
// connections. channel_id defaults to 0 when ASTRA does not specify one.
struct ConnectionKey {
  int src, dst;
  int pg;
  uint64_t channel;
  bool operator<(const ConnectionKey &o) const {
    if (src != o.src) return src < o.src;
    if (dst != o.dst) return dst < o.dst;
    if (pg != o.pg) return pg < o.pg;
    return channel < o.channel;
  }
};

struct PersistentConnection {
  uint64_t conn_id = 0;
  ConnectionKey key;
  uint16_t sport = 0, dport = 0;
  Ptr<RdmaQueuePair> qp;
  Time create_time;
  uint64_t messages_posted = 0;
  uint64_t messages_completed = 0;
  std::string path;
};

map<ConnectionKey, PersistentConnection *> connections;
map<uint64_t, PersistentConnection *> connections_by_id;
map<uint64_t, LogicalMessage> live_messages;              // persistent mode, keyed by msg_id
map<pair<int, pair<int, int>>, LogicalMessage> legacy_messages; // legacy mode, keyed by (sport,(src,dst))
uint64_t next_msg_id = 1;
uint64_t next_conn_id = 1;
uint64_t frontend_sends_issued = 0;
FILE *flow_trace_out = NULL;
FILE *path_trace_out = NULL;
FILE *fct_output_persistent = NULL;
bool backend_finalized = false;

bool persistent_mode() { return transport_connection_mode == "PERSISTENT"; }

std::string meta_field(uint64_t v) {
  if (v == META_UNSPECIFIED)
    return "";
  return std::to_string(v);
}

// Replicates the host NIC choice and every switch's ECMP decision for a UDP
// data flow, using the switches' own hash (SwitchNode::LookupDataNextHop)
// and the routing tables built by CalculateRoutes. Returns
// "src:if>switch:if>...>dst". Does not modify any state.
std::string compute_flow_path(int src, int dst, uint16_t sport, uint16_t dport) {
  uint32_t sip = serverAddress[src].Get(), dip = serverAddress[dst].Get();
  Ptr<Node> node = n.Get(src);
  std::ostringstream out;
  // host NIC selection: RdmaHw::GetNicIdxOfQp hashes the 4-tuple over m_rtTable[dip]
  Ptr<RdmaDriver> rdma = node->GetObject<RdmaDriver>();
  if (!rdma || !rdma->m_rdma)
    return "";
  auto rt = rdma->m_rdma->m_rtTable.find(dip);
  if (rt == rdma->m_rdma->m_rtTable.end() || rt->second.empty())
    return "";
  const std::vector<int> &v = rt->second;
  union {
    struct { uint32_t sip, dip; uint16_t sport, dport; };
    char c[12];
  } buf;
  buf.sip = sip; buf.dip = dip; buf.sport = sport; buf.dport = dport;
  uint32_t ifidx = v[Hash32(buf.c, 12) % v.size()];
  out << src << ":" << ifidx;
  for (int hop = 0; hop < 64; hop++) {
    Ptr<Node> next = NULL;
    for (auto &kv : nbr2if[node]) {
      if (kv.second.idx == ifidx) { next = kv.first; break; }
    }
    if (!next)
      return out.str() + ">?";
    if (next->GetNodeType() != 1) {
      out << ">" << next->GetId();
      return out.str();
    }
    Ptr<SwitchNode> sw = DynamicCast<SwitchNode>(next);
    int outIf = sw->LookupDataNextHop(sip, dip, sport, dport);
    if (outIf < 0)
      return out.str() + ">" + std::to_string(next->GetId()) + ":?";
    out << ">" << next->GetId() << ":" << outIf;
    node = next;
    ifidx = (uint32_t)outIf;
  }
  return out.str() + ">...";
}

void open_frontend_logs() {
  if (enable_flow_metadata_trace && !flow_trace_out) {
    flow_trace_out = fopen(flow_trace_file.c_str(), "w");
    if (flow_trace_out)
      fprintf(flow_trace_out,
              "msg_id,kind,collective_id,operation_id,phase_id,chunk_id,communicator_id,"
              "channel_id,connection_id,qp_id,src,dst,sport,dport,tag,bytes,issue_time_ns,"
              "network_start_time_ns,completion_time_ns,path\n");
  }
  if (enable_path_trace && !path_trace_out) {
    path_trace_out = fopen(path_trace_file.c_str(), "w");
    if (path_trace_out)
      fprintf(path_trace_out, "record,connection_id,qp_id,src,dst,sport,dport,time_ns,path\n");
  }
}

void log_flow_row(const LogicalMessage &m) {
  if (!flow_trace_out)
    return;
  fprintf(flow_trace_out, "%lu,%s,%s,%s,%s,%s,%s,%s,%lu,%lu,%d,%d,%u,%u,%d,%lu,%lu,%lu,%lu,%s\n",
          (unsigned long)m.msg_id, m.handshake ? "handshake" : "payload",
          meta_field(m.meta.collective_id).c_str(), meta_field(m.meta.operation_id).c_str(),
          meta_field(m.meta.phase_id).c_str(), meta_field(m.meta.chunk_id).c_str(),
          meta_field(m.meta.communicator_id).c_str(), meta_field(m.meta.channel_id).c_str(),
          (unsigned long)m.conn_id, (unsigned long)m.qp_id, m.src, m.dst, m.sport, m.dport,
          m.tag, (unsigned long)m.bytes, (unsigned long)m.issue_time.GetTimeStep(),
          (unsigned long)m.network_start_time.GetTimeStep(),
          (unsigned long)m.completion_time.GetTimeStep(), m.path.c_str());
}

void log_path_row(const char *record, uint64_t conn_id, uint64_t qp_id, int src, int dst,
                  uint16_t sport, uint16_t dport, const std::string &path) {
  if (!path_trace_out)
    return;
  fprintf(path_trace_out, "%s,%lu,%lu,%d,%d,%u,%u,%lu,%s\n", record, (unsigned long)conn_id,
          (unsigned long)qp_id, src, dst, sport, dport,
          (unsigned long)Simulator::Now().GetTimeStep(), path.c_str());
}

/*********************************************************************
 * LEGACY data path (unchanged semantics)
 ********************************************************************/

// send_flow commands the ns3 simulator to schedule a RDMA message to be sent
// between two pair of nodes. send_flow is triggered by sim_send.
void send_flow_legacy(int src_id, int dst, int maxPacketCount,
               void (*msg_handler)(void *fun_arg), void *fun_arg, int tag,
               const MsgMeta &meta, uint64_t msg_id) {
  // Get a new port number.
  uint32_t port = portNumber[src_id][dst]++;
  sender_src_port_map[make_pair(port, make_pair(src_id, dst))] = tag;
  int pg = 3, dport = 100;
  flow_input.idx++;

  // Create a MsgEvent instance and register callback function.
  MsgEvent send_event =
      MsgEvent(src_id, dst, 0, maxPacketCount, fun_arg, msg_handler);
  pair<MsgEventKey, int> send_event_key =
      make_pair(make_pair(tag, make_pair(send_event.src_id, send_event.dst_id)),port) ;
  sim_send_waiting_hash[send_event_key] = send_event;

  // observational bookkeeping for the flow/path logs
  if (flow_trace_out || path_trace_out) {
    LogicalMessage m;
    m.msg_id = msg_id;
    m.src = src_id;
    m.dst = dst;
    m.bytes = maxPacketCount;
    m.tag = tag;
    m.handshake = tag >= RENDEZVOUS_TAG_OFFSET;
    m.meta = meta;
    m.sport = port;
    m.dport = dport;
    m.issue_time = Simulator::Now();
    if (enable_path_trace) {
      m.path = compute_flow_path(src_id, dst, port, dport);
      log_path_row("flow", 0, 0, src_id, dst, port, dport, m.path);
    }
    legacy_messages[make_pair(port, make_pair(src_id, dst))] = m;
  }

  // Create a queue pair and schedule within the ns3 simulator.
  RdmaClientHelper clientHelper(
      pg, serverAddress[src_id], serverAddress[dst], port, dport,
      maxPacketCount,
      has_win ? (global_t == 1 ? maxBdp : pairBdp[n.Get(src_id)][n.Get(dst)])
              : 0,
      global_t == 1 ? maxRtt : pairRtt[src_id][dst], msg_handler, fun_arg, tag,
      src_id, dst);
  ApplicationContainer appCon = clientHelper.Install(n.Get(src_id));
  appCon.Start(Time(0));
}

// notify_receiver_receive_data looks at whether the System layer has issued
// sim_recv for this message. If the system layer is waiting for this message,
// call the callback handler for the MsgEvent. If the system layer is not *yet*
// waiting for this message, register that this message has arrived,
// so that the system layer can later call the callback handler when sim_recv
// is called.
void notify_receiver_receive_data(int src_id,
                                  int dst_id,
                                  int message_size,
                                  int tag) {

    MsgEventKey recv_expect_event_key =
        make_pair(tag, make_pair(src_id, dst_id));

    int remaining_bytes = message_size;
    auto waiting_it = sim_recv_waiting_hash.find(recv_expect_event_key);
    if (waiting_it != sim_recv_waiting_hash.end()) {
        // Consume the received bytes against pending receive events in FIFO
        // order.
        queue<MsgEvent>& waiting_events = waiting_it->second;
        while (!waiting_events.empty()) {
            MsgEvent& recv_expect_event = waiting_events.front();
            if (remaining_bytes < recv_expect_event.remaining_msg_bytes) {
                recv_expect_event.remaining_msg_bytes -= remaining_bytes;
                remaining_bytes = 0;
                break;
            } else {
                remaining_bytes -= recv_expect_event.remaining_msg_bytes;
                MsgEvent completed_event = recv_expect_event;
                waiting_events.pop();
                completed_event.callHandler();
            }
        }

        if (waiting_events.empty()) {
            sim_recv_waiting_hash.erase(waiting_it);
        }
    }

    if (remaining_bytes > 0) {
        // The Sys object is not yet waiting for all of the received bytes.
        received_msg_standby_hash[recv_expect_event_key] += remaining_bytes;
    }

    // Add to the number of total bytes received.
    if (node_to_bytes_sent_map.find(make_pair(dst_id, 1)) ==
        node_to_bytes_sent_map.end()) {
        node_to_bytes_sent_map[make_pair(dst_id, 1)] = message_size;
    } else {
        node_to_bytes_sent_map[make_pair(dst_id, 1)] += message_size;
    }
}

void notify_sender_sending_finished(int src_id, int dst_id, int message_size,
                                    int tag, int src_port) {
  // Lookup the send_event registered at send_flow().
  pair<MsgEventKey, int> send_event_key = make_pair(make_pair(tag, make_pair(src_id, dst_id)), src_port);
  if (sim_send_waiting_hash.find(send_event_key) == sim_send_waiting_hash.end()) {
    cerr << "Cannot find send_event in sent_hash. Something is wrong."
         << "tag, src_id, dst_id: " << tag << " " << src_id << " " << dst_id
         << "\n";
    exit(1);
  }

  // Verify that the (ns3 identified) sent message size matches what was
  // expected by the system layer.
  MsgEvent send_event = sim_send_waiting_hash[send_event_key];
  if (send_event.remaining_msg_bytes != message_size) {
    cerr << "The message size does not match what is expected. Something is "
            "wrong."
         << "tag, src_id, dst_id, expected msg_bytes, actual msg_bytes: " << tag
         << " " << src_id << " " << dst_id << " "
         << send_event.remaining_msg_bytes << " " << message_size << "\n";
    exit(1);
  }
  sim_send_waiting_hash.erase(send_event_key);

  // Add to the number of total bytes sent.
  if (node_to_bytes_sent_map.find(make_pair(src_id, 0)) == node_to_bytes_sent_map.end()) {
    node_to_bytes_sent_map[make_pair(src_id, 0)] = message_size;
  } else {
    node_to_bytes_sent_map[make_pair(src_id, 0)] += message_size;
  }
  send_event.callHandler();
}

void qp_finish_print_log(FILE *fout, Ptr<RdmaQueuePair> q) {
  uint32_t sid = ip_to_node_id(q->sip), did = ip_to_node_id(q->dip);
  uint64_t base_rtt = pairRtt[sid][did], b = pairBw[sid][did];
  uint32_t total_bytes =
      q->m_size +
      ((q->m_size - 1) / packet_payload_size + 1) *
          (CustomHeader::GetStaticWholeHeaderSize() -
           IntHeader::GetStaticSize()); // translate to the minimum bytes
                                        // required (with header but no INT)
  uint64_t standalone_fct = base_rtt + total_bytes * 8000000000lu / b;
  // sip, dip, sport, dport, size (B), start_time, fct (ns), standalone_fct (ns)
  fprintf(fout, "%08x %08x %u %u %lu %lu %lu %lu\n", q->sip.Get(), q->dip.Get(),
          q->sport, q->dport, q->m_size, q->startTime.GetTimeStep(),
          (Simulator::Now() - q->startTime).GetTimeStep(), standalone_fct);
  fflush(fout);
}

// qp_finish is triggered by NS3 to indicate that an RDMA queue pair has
// finished. qp_finish is registered as the callback handlerto the RdmaClient
// instance created at send_flow. This registration is done at
// common.h::SetupNetwork().
void qp_finish(FILE *fout, Ptr<RdmaQueuePair> q) {
  uint32_t sid = ip_to_node_id(q->sip), did = ip_to_node_id(q->dip);
  qp_finish_print_log(fout, q);

  // remove rxQp from the receiver.
  Ptr<Node> dstNode = n.Get(did);
  Ptr<RdmaDriver> rdma = dstNode->GetObject<RdmaDriver>();
  rdma->m_rdma->DeleteRxQp(q->sip.Get(), q->m_pg, q->sport);

  // Identify the tag of this message.
  if (sender_src_port_map.find(make_pair(q->sport, make_pair(sid, did))) ==
      sender_src_port_map.end()) {
    cout << "could not find the tag, there must be something wrong" << endl;
    exit(-1);
  }
  int tag = sender_src_port_map[make_pair(q->sport, make_pair(sid, did))];
  sender_src_port_map.erase(make_pair(q->sport, make_pair(sid, did)));

  // observational: flow log row for this legacy flow
  if (flow_trace_out) {
    auto it = legacy_messages.find(make_pair(q->sport, make_pair((int)sid, (int)did)));
    if (it != legacy_messages.end()) {
      it->second.qp_id = q->m_qp_id;
      it->second.network_start_time = q->startTime;
      it->second.completion_time = Simulator::Now();
      log_flow_row(it->second);
      legacy_messages.erase(it);
    }
  } else if (path_trace_out) {
    legacy_messages.erase(make_pair(q->sport, make_pair((int)sid, (int)did)));
  }

  // Let sender knows that the flow has finished.
  notify_sender_sending_finished(sid, did, q->m_size, tag, q->sport);

  // Let receiver knows that it has received packets.
  notify_receiver_receive_data(sid, did, q->m_size, tag);
}

/*********************************************************************
 * PERSISTENT data path
 ********************************************************************/

void persistent_message_complete(Ptr<RdmaQueuePair> q, RdmaWorkRequest wr);

PersistentConnection *get_or_create_connection(int src_id, int dst, const MsgMeta &meta) {
  ConnectionKey key;
  key.src = src_id;
  key.dst = dst;
  key.pg = 3;
  key.channel = meta.channel_id == META_UNSPECIFIED ? 0 : meta.channel_id;
  auto it = connections.find(key);
  if (it != connections.end())
    return it->second;

  PersistentConnection *conn = new PersistentConnection();
  conn->conn_id = next_conn_id++;
  conn->key = key;
  // Same allocator as LEGACY so the first connection of a pair uses the port
  // the first legacy flow would have used.
  conn->sport = portNumber[src_id][dst]++;
  conn->dport = 100;
  conn->create_time = Simulator::Now();
  Ptr<RdmaDriver> rdma = n.Get(src_id)->GetObject<RdmaDriver>();
  conn->qp = rdma->m_rdma->CreatePersistentQp(
      src_id, dst, key.pg, serverAddress[src_id], serverAddress[dst], conn->sport, conn->dport,
      has_win ? (global_t == 1 ? maxBdp : pairBdp[n.Get(src_id)][n.Get(dst)]) : 0,
      global_t == 1 ? maxRtt : pairRtt[src_id][dst], conn->conn_id,
      MakeCallback(&persistent_message_complete));
  if (enable_path_trace) {
    conn->path = compute_flow_path(src_id, dst, conn->sport, conn->dport);
    log_path_row("connection", conn->conn_id, conn->qp->m_qp_id, src_id, dst, conn->sport,
                 conn->dport, conn->path);
  }
  connections[key] = conn;
  connections_by_id[conn->conn_id] = conn;
  return conn;
}

// Deferred by one zero-delay event, as the legacy RdmaClient start is, so the
// post happens outside whatever ACK/callback context issued the sim_send.
void post_persistent_message(uint64_t msg_id) {
  auto it = live_messages.find(msg_id);
  if (it == live_messages.end())
    return;
  LogicalMessage &m = it->second;
  PersistentConnection *conn = connections_by_id[m.conn_id];
  Ptr<RdmaDriver> rdma = n.Get(m.src)->GetObject<RdmaDriver>();
  rdma->m_rdma->PostMessage(conn->qp, msg_id, m.bytes);
  conn->messages_posted++;
}

void send_flow_persistent(int src_id, int dst, uint64_t bytes,
                          void (*msg_handler)(void *fun_arg), void *fun_arg, int tag,
                          const MsgMeta &meta, uint64_t msg_id) {
  PersistentConnection *conn = get_or_create_connection(src_id, dst, meta);
  LogicalMessage m;
  m.msg_id = msg_id;
  m.src = src_id;
  m.dst = dst;
  m.bytes = bytes;
  m.tag = tag;
  m.handshake = tag >= RENDEZVOUS_TAG_OFFSET;
  m.meta = meta;
  m.conn_id = conn->conn_id;
  m.qp_id = conn->qp->m_qp_id;
  m.sport = conn->sport;
  m.dport = conn->dport;
  m.issue_time = Simulator::Now();
  m.fun_arg = fun_arg;
  m.msg_handler = msg_handler;
  m.path = conn->path;
  live_messages[msg_id] = m;
  flow_input.idx++;
  Simulator::ScheduleNow(&post_persistent_message, msg_id);
}

void persistent_message_print_log(FILE *fout, Ptr<RdmaQueuePair> q, const LogicalMessage &m,
                                  const RdmaWorkRequest &wr) {
  uint32_t sid = ip_to_node_id(q->sip), did = ip_to_node_id(q->dip);
  uint64_t base_rtt = pairRtt[sid][did], b = pairBw[sid][did];
  uint64_t size = m.bytes;
  uint64_t total_bytes =
      size + ((size - 1) / packet_payload_size + 1) *
                 (CustomHeader::GetStaticWholeHeaderSize() - IntHeader::GetStaticSize());
  uint64_t standalone_fct = base_rtt + total_bytes * 8000000000lu / b;
  // Same columns as the legacy fct.txt line: sip dip sport dport size start fct standalone.
  // start is the time the message was posted to the QP.
  fprintf(fout, "%08x %08x %u %u %lu %lu %lu %lu\n", q->sip.Get(), q->dip.Get(), q->sport,
          q->dport, (unsigned long)size, (unsigned long)wr.postTime.GetTimeStep(),
          (unsigned long)(Simulator::Now() - wr.postTime).GetTimeStep(),
          (unsigned long)standalone_fct);
  fflush(fout);
}

void persistent_message_complete(Ptr<RdmaQueuePair> q, RdmaWorkRequest wr) {
  auto it = live_messages.find(wr.msg_id);
  if (it == live_messages.end()) {
    cerr << "persistent_message_complete: unknown message " << wr.msg_id << "\n";
    exit(1);
  }
  LogicalMessage m = it->second;
  live_messages.erase(it);
  m.completion_time = Simulator::Now();
  m.network_start_time = wr.firstTxTime;
  PersistentConnection *conn = connections_by_id[m.conn_id];
  conn->messages_completed++;

  if (fct_output_persistent)
    persistent_message_print_log(fct_output_persistent, q, m, wr);
  log_flow_row(m);

  // Sender side: same bookkeeping as notify_sender_sending_finished, keyed by
  // message id because messages on one connection share tag and port.
  if (node_to_bytes_sent_map.find(make_pair(m.src, 0)) == node_to_bytes_sent_map.end()) {
    node_to_bytes_sent_map[make_pair(m.src, 0)] = m.bytes;
  } else {
    node_to_bytes_sent_map[make_pair(m.src, 0)] += m.bytes;
  }
  m.msg_handler(m.fun_arg);

  // Receiver side: unchanged.
  notify_receiver_receive_data(m.src, m.dst, m.bytes, m.tag);
}

/*********************************************************************
 * Entry points
 ********************************************************************/

void send_flow(int src_id, int dst, uint64_t message_size,
               void (*msg_handler)(void *fun_arg), void *fun_arg, int tag,
               const MsgMeta &meta) {
  frontend_sends_issued++;
  uint64_t msg_id = next_msg_id++;
  if (persistent_mode()) {
    send_flow_persistent(src_id, dst, message_size, msg_handler, fun_arg, tag, meta, msg_id);
  } else {
    send_flow_legacy(src_id, dst, (int)message_size, msg_handler, fun_arg, tag, meta, msg_id);
  }
}

// Tear down persistent connections, flush logs, write counters. Called once,
// either when every rank has finished or when the event queue drains.
void finalize_backend(bool all_ranks_finished) {
  if (backend_finalized)
    return;
  backend_finalized = true;
  for (auto &kv : connections) {
    PersistentConnection *conn = kv.second;
    if (path_trace_out || flow_trace_out) {
      // connection summary row in the path trace (also serves as the
      // connection lifetime record: create time, destroy time, messages)
      if (path_trace_out)
        fprintf(path_trace_out, "connection_end,%lu,%lu,%d,%d,%u,%u,%lu,created=%lu;messages=%lu;%s\n",
                (unsigned long)conn->conn_id, (unsigned long)conn->qp->m_qp_id, conn->key.src,
                conn->key.dst, conn->sport, conn->dport,
                (unsigned long)Simulator::Now().GetTimeStep(),
                (unsigned long)conn->create_time.GetTimeStep(),
                (unsigned long)conn->messages_completed, conn->path.c_str());
    }
    Ptr<RdmaDriver> src_rdma = n.Get(conn->key.src)->GetObject<RdmaDriver>();
    src_rdma->m_rdma->DestroyPersistentQp(conn->qp);
    Ptr<RdmaDriver> dst_rdma = n.Get(conn->key.dst)->GetObject<RdmaDriver>();
    dst_rdma->m_rdma->DeleteRxQp(serverAddress[conn->key.src].Get(), conn->key.pg, conn->sport);
  }
  uint64_t outstanding = frontend_sends_issued - RdmaCounters::messages_completed;
  printf("backend counters: transport_connection_mode=%s sends_issued=%lu messages_accepted=%lu "
         "messages_completed=%lu outstanding=%lu connections=%lu qps_created=%lu "
         "qps_destroyed=%lu ecn_marks=%lu cnp_acks=%lu mmu_drops=%lu pfc_pause_frames=%lu "
         "all_ranks_finished=%d\n",
         transport_connection_mode.c_str(), (unsigned long)frontend_sends_issued,
         (unsigned long)RdmaCounters::messages_posted,
         (unsigned long)RdmaCounters::messages_completed, (unsigned long)outstanding,
         (unsigned long)connections.size(), (unsigned long)RdmaCounters::qps_created,
         (unsigned long)RdmaCounters::qps_destroyed, (unsigned long)RdmaCounters::ecn_marks,
         (unsigned long)RdmaCounters::cnp_acks, (unsigned long)RdmaCounters::mmu_drops,
         (unsigned long)RdmaCounters::pfc_pause_frames, all_ranks_finished ? 1 : 0);
  if (!counters_output_file.empty()) {
    FILE *out = fopen(counters_output_file.c_str(), "w");
    if (out) {
      fprintf(out, "transport_connection_mode=%s\n", transport_connection_mode.c_str());
      fprintf(out, "all_ranks_finished=%d\n", all_ranks_finished ? 1 : 0);
      fprintf(out, "sends_issued=%lu\n", (unsigned long)frontend_sends_issued);
      fprintf(out, "messages_accepted=%lu\n", (unsigned long)RdmaCounters::messages_posted);
      fprintf(out, "messages_completed=%lu\n", (unsigned long)RdmaCounters::messages_completed);
      fprintf(out, "messages_outstanding=%lu\n", (unsigned long)outstanding);
      fprintf(out, "connections_created=%lu\n", (unsigned long)connections.size());
      fprintf(out, "qps_created=%lu\n", (unsigned long)RdmaCounters::qps_created);
      fprintf(out, "qps_destroyed=%lu\n", (unsigned long)RdmaCounters::qps_destroyed);
      fprintf(out, "ecn_marks=%lu\n", (unsigned long)RdmaCounters::ecn_marks);
      fprintf(out, "cnp_acks=%lu\n", (unsigned long)RdmaCounters::cnp_acks);
      fprintf(out, "mmu_drops=%lu\n", (unsigned long)RdmaCounters::mmu_drops);
      fprintf(out, "pfc_pause_frames=%lu\n", (unsigned long)RdmaCounters::pfc_pause_frames);
      fprintf(out, "pfc_resume_frames=%lu\n", (unsigned long)RdmaCounters::pfc_resume_frames);
      fprintf(out, "end_time_ns=%lu\n", (unsigned long)Simulator::Now().GetTimeStep());
      for (uint32_t i = 0; i < n.GetN(); i++) {
        if (n.Get(i)->GetNodeType() != 1)
          continue;
        Ptr<SwitchNode> sw = DynamicCast<SwitchNode>(n.Get(i));
        for (uint32_t j = 1; j < sw->GetNDevices(); j++)
          if (sw->GetTxBytes(j) || sw->m_ecnMarks[j] || sw->m_mmuDrops[j] || sw->m_pfcPauseTx[j])
            fprintf(out, "switch_port node=%u ifindex=%u tx_bytes=%lu ecn_marks=%lu mmu_drops=%lu pfc_pause_tx=%lu\n",
                    i, j, (unsigned long)sw->GetTxBytes(j), (unsigned long)sw->m_ecnMarks[j],
                    (unsigned long)sw->m_mmuDrops[j], (unsigned long)sw->m_pfcPauseTx[j]);
      }
      fclose(out);
    }
  }
  if (flow_trace_out) { fflush(flow_trace_out); fclose(flow_trace_out); flow_trace_out = NULL; }
  if (path_trace_out) { fflush(path_trace_out); fclose(path_trace_out); path_trace_out = NULL; }
  if (fct_output_persistent) { fflush(fct_output_persistent); fclose(fct_output_persistent); fct_output_persistent = NULL; }
  RdmaCcTrace::Close();
  fflush(stdout);
}

int setup_ns3_simulation(string network_configuration) {
  if (!ReadConf(network_configuration))
    return -1;

  SetConfig();

  if (!SetupNetwork(qp_finish)) {
    return -1;
  }

  open_frontend_logs();
  if (persistent_mode()) {
    // Persistent messages are logged to the same fct file the legacy path
    // uses (opened by SetupNetwork). Reopen by name in append mode so the
    // two paths never hold the file twice.
    fct_output_persistent = fopen(fct_output_file.c_str(), "a");
  }
  printf("transport_connection_mode=%s enable_pfc=%u pfc_threshold_validation=%s "
         "queue_log_min_bytes=%u trace_level=%u\n",
         transport_connection_mode.c_str(), enable_pfc, pfc_threshold_validation.c_str(),
         queue_log_min_bytes, trace_level);
  return 0;

}
