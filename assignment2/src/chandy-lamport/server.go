package chandy_lamport

import "log"

// The main participant of the distributed snapshot protocol.
// Servers exchange token messages and marker messages among each other.
// Token messages represent the transfer of tokens from one server to another.
// Marker messages represent the progress of the snapshot process. The bulk of
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.
type Server struct {
	Id            string
	Tokens        int
	sim           *Simulator
	outboundLinks map[string]*Link // key = link.dest
	inboundLinks  map[string]*Link // key = link.src
	// TODO: ADD MORE FIELDS HERE
	localSnapshot *SyncMap
	// MarkerReceiver Map (int, Map(snapshotID, serverID))
	markerReceived map[int]map[string]bool
}

// A unidirectional communication channel between two servers
// Each link contains an event queue (as opposed to a packet queue)
type Link struct {
	src    string
	dest   string
	events *Queue
}

func NewServer(id string, tokens int, sim *Simulator) *Server {
	return &Server{
		id,
		tokens,
		sim,
		make(map[string]*Link),
		make(map[string]*Link),
		NewSyncMap(),
		make(map[int]map[string]bool),
	}
}

// Add a unidirectional link to the destination server
func (server *Server) AddOutboundLink(dest *Server) {
	if server == dest {
		return
	}
	l := Link{server.Id, dest.Id, NewQueue()}
	server.outboundLinks[dest.Id] = &l
	dest.inboundLinks[server.Id] = &l
}

// Send a message on all of the server's outbound links
func (server *Server) SendToNeighbors(message interface{}) {
	for _, serverId := range getSortedKeys(server.outboundLinks) {
		link := server.outboundLinks[serverId]
		server.sim.logger.RecordEvent(
			server,
			SentMessageEvent{server.Id, link.dest, message})
		link.events.Push(SendMessageEvent{
			server.Id,
			link.dest,
			message,
			server.sim.GetReceiveTime()})
	}
}

// Send a number of tokens to a neighbor attached to this server
func (server *Server) SendTokens(numTokens int, dest string) {
	if server.Tokens < numTokens {
		log.Fatalf("Server %v attempted to send %v tokens when it only has %v\n",
			server.Id, numTokens, server.Tokens)
	}
	message := TokenMessage{numTokens}
	server.sim.logger.RecordEvent(server, SentMessageEvent{server.Id, dest, message})
	// Update local state before sending the tokens
	server.Tokens -= numTokens
	link, ok := server.outboundLinks[dest]
	if !ok {
		log.Fatalf("Unknown dest ID %v from server %v\n", dest, server.Id)
	}
	link.events.Push(SendMessageEvent{
		server.Id,
		dest,
		message,
		server.sim.GetReceiveTime()})
}

// Callback for when a message is received on this server.
// When the snapshot algorithm completes on this server, this function
// should notify the simulator by calling `sim.NotifySnapshotComplete`.
func (server *Server) HandlePacket(src string, message interface{}) {
	// TODO: IMPLEMENT ME
	switch msg := message.(type) {
	case TokenMessage:
		// We have to add the received tokens to the counter
		server.Tokens += msg.numTokens
		ids := make([]int, 0)

		server.localSnapshot.Range(func(key, value interface{}) bool {
			snapShotId := key.(int)
			if !server.markerReceived[snapShotId][src] { // If haven't received marker from src for that snapshots
				ids = append(ids, snapShotId) // Then add the snapshot id to the id's slice
			}
			return true

		})

		message := &SnapshotMessage{src, server.Id, msg} // Create a new message struct

		for _, id := range ids {
			tempSnap, _ := server.localSnapshot.Load(id)
			snap := tempSnap.(SnapshotState)
			snap.messages = append(snap.messages, message) //Adding the new message to the snapshot
			server.localSnapshot.Store(id, snap)
		}
	case MarkerMessage:
		_, ok := server.localSnapshot.Load(msg.snapshotId) // Read the local snapshot if exists
		if !ok {
			server.StartSnapshot(msg.snapshotId) // If not exists the start it
		}
		server.markerReceived[msg.snapshotId][src] = true // Marker received from source, so set flag to true

		if len(server.markerReceived[msg.snapshotId]) == len(server.inboundLinks) { // When received a marker from all the inbound links
			server.sim.NotifySnapshotComplete(server.Id, msg.snapshotId) // Notify to the simulator
		}

	}
}

// Start the chandy-lamport snapshot algorithm on this server.
// This should be called only once per server.
func (server *Server) StartSnapshot(snapshotId int) {
	// TODO: IMPLEMENT ME
	snapshot := SnapshotState{snapshotId, make(map[string]int), make([]*SnapshotMessage, 0)} //Create new local snapshot
	snapshot.tokens[server.Id] = server.Tokens                                               // Save the current num of tokens
	server.localSnapshot.Store(snapshotId, snapshot)
	server.markerReceived[snapshotId] = make(map[string]bool) //Add the snapshot id to the markerReceived map
	server.SendToNeighbors(MarkerMessage{snapshotId})         //Send the marker to all the neighbors
}
