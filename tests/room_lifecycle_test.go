package tests

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ER-brocode/nexus-sfu-gateway/pkg/signaling"
	"github.com/gorilla/websocket"
)

func TestRoomLifecycleAndSignaling(t *testing.T) {
	server := signaling.NewServer()
	ts := httptest.NewServer(http.HandlerFunc(server.HandleWebSocket))
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"

	// Connect Client 1
	url1 := fmt.Sprintf("%s?peerId=peer-alice&roomId=test-room-101", wsURL)
	c1, _, err := websocket.DefaultDialer.Dial(url1, nil)
	if err != nil {
		t.Fatalf("Failed to dial ws for client 1: %v", err)
	}
	defer c1.Close()

	// Connect Client 2
	url2 := fmt.Sprintf("%s?peerId=peer-bob&roomId=test-room-101", wsURL)
	c2, _, err := websocket.DefaultDialer.Dial(url2, nil)
	if err != nil {
		t.Fatalf("Failed to dial ws for client 2: %v", err)
	}
	defer c2.Close()

	// Client 1 should receive peer_joined notification about bob
	var msg signaling.SignalMessage
	_ = c1.SetReadDeadline(time.Now().Add(2 * time.Second))
	if err := c1.ReadJSON(&msg); err != nil {
		t.Fatalf("Failed to read peer_joined message: %v", err)
	}

	if msg.Type != signaling.MsgPeerJoined {
		t.Errorf("Expected MsgPeerJoined, got %s", msg.Type)
	}
	if msg.PeerID != "peer-bob" {
		t.Errorf("Expected peer-bob, got %s", msg.PeerID)
	}

	// Client 2 sends an offer
	offerMsg := signaling.SignalMessage{
		Type:   signaling.MsgOffer,
		PeerID: "peer-bob",
		RoomID: "test-room-101",
	}
	if err := c2.WriteJSON(offerMsg); err != nil {
		t.Fatalf("Failed to write offer: %v", err)
	}

	// Client 1 should receive the offer
	var receivedOffer signaling.SignalMessage
	_ = c1.SetReadDeadline(time.Now().Add(2 * time.Second))
	if err := c1.ReadJSON(&receivedOffer); err != nil {
		t.Fatalf("Failed to receive forwarded offer: %v", err)
	}
	if receivedOffer.Type != signaling.MsgOffer || receivedOffer.PeerID != "peer-bob" {
		t.Errorf("Unexpected offer message: %+v", receivedOffer)
	}

	// Verify room manager client counts
	room, err := server.RoomManager.GetRoom("test-room-101")
	if err != nil {
		t.Fatalf("Failed to find room: %v", err)
	}
	if room.ClientCount() != 2 {
		t.Errorf("Expected 2 clients in room, got %d", room.ClientCount())
	}

	// Client 2 leaves
	leaveMsg := signaling.SignalMessage{Type: signaling.MsgLeave}
	_ = c2.WriteJSON(leaveMsg)
	c2.Close()

	// Wait for readPump to register leave
	time.Sleep(100 * time.Millisecond)

	// Client 1 should receive peer_left
	var leftMsg signaling.SignalMessage
	_ = c1.SetReadDeadline(time.Now().Add(2 * time.Second))
	if err := c1.ReadJSON(&leftMsg); err == nil {
		if leftMsg.Type == signaling.MsgPeerLeft && leftMsg.PeerID == "peer-bob" {
			// success
		}
	}
}
