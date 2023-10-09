package raft

import (
	"log"
	"testing"
	"time"
)

func TestServersUnreliableElectionsMe(t *testing.T) {
	servers := 10
	cfg := make_config(t, servers, true, false)
	defer cfg.cleanup()

	cfg.begin("Test (3A): more servers, more elections, unreliable connection")

	cfg.disconnect(0)
	prevLeader := 0

	// disconnect leader, reconnect prev leader
	for i := 0; i < 5*servers; i++ {
		time.Sleep(50 * time.Millisecond)

		currentLeader := cfg.checkOneLeader()

		cfg.connect(prevLeader)
		prevLeader = currentLeader
		cfg.disconnect(currentLeader)
	}

	cfg.setunreliable(false)
	for i := 0; i < servers; i++ {
		cfg.connect(i)
	}

	// give servers some time to agree on new term
	// specifically, just barely more than the 1/10 of a second allowed for leaders to send out heartbeats
	time.Sleep(110 * time.Millisecond)

	term := cfg.checkTerms()
	if term < 5*servers+1 {
		t.Fatalf("term: %v, should be at least %v", term, servers)
	}

	cfg.end()
}

func TestConnectDisconnectMe(t *testing.T) {
	servers := 10
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (3A): various disconnect/reconnects")

	leader1 := cfg.checkOneLeader()
	cfg.disconnect(leader1)
	cfg.connect(leader1)
	leader2 := cfg.checkOneLeader()

	// brief disconnect and reconnect should not provoke new election
	if leader1 != leader2 {
		t.Fatalf("leader brief disconnect/reconnect caused new election")
	}

	// disconnecting non-leader servers should not cause leader to step down
	for i := 0; i < servers; i++ {
		if i != leader2 {
			cfg.disconnect(i)
			cfg.checkOneLeader()
		}
	}

	// disconnecting all servers should result in no leader
	cfg.crash1(leader2)
	cfg.checkNoLeader()

	// leader2 should no longer be the leader after crashing and restarting
	cfg.start1(leader2, cfg.applier)
	cfg.checkNoLeader()

	cfg.end()
}

func TestLongLogPropagateMe(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (3B): propagating long logs to reconnected leader")

	leader1 := cfg.checkOneLeader()

	iters := 1000
	for i := 0; i < iters; i++ {
		cfg.rafts[leader1].Start(i)
		time.Sleep(time.Millisecond)
		// log.Printf("%d first add", i)
	}

	cfg.disconnect(leader1)

	// other followers should elect new leader
	time.Sleep(1000 * time.Millisecond)

	leader2 := cfg.checkOneLeader()

	for i := 0; i < iters; i++ {
		cfg.rafts[leader2].Start(i + iters)
		time.Sleep(time.Millisecond)
		// log.Printf("%d second add", i)
	}

	term := cfg.checkTerms()

	// wait for starts to finish
	for i := 1; i < 2*iters; i++ {
		cfg.wait(i, servers-1, term)
	}

	cfg.connect(leader1)

	// wait for new election to occur
	time.Sleep(1000 * time.Millisecond)
	// log.Printf("new leader %d", cfg.checkOneLeader())
	time.Sleep(1000 * time.Millisecond)
	term = cfg.checkTerms()

	// logs should propagate to leader quickly
	ti := time.Now()
	for i := 1; i < 2*iters+1; i++ {
		cfg.wait(i, servers, term)
	}

	elapsed := time.Since(ti)
	if elapsed > time.Second {
		t.Fatalf("leader log propagation took too long: %v", elapsed)
	}

	cfg.end()
}

func TestStartDisconnectedLeaderMe(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (3B): submitting starts to disconnected leader")

	leader1 := cfg.checkOneLeader()

	iters := 100
	for i := 0; i < iters; i++ {
		cfg.rafts[leader1].Start(i)
		time.Sleep(time.Millisecond)
		// log.Printf("%d i", i)

	}

	cfg.disconnect(leader1)

	// other followers should elect new leader
	time.Sleep(1000 * time.Millisecond)

	leader2 := cfg.checkOneLeader()

	for i := 0; i < iters; i++ {
		cfg.rafts[leader2].Start(i + iters)
		time.Sleep(time.Millisecond)
	}

	term := cfg.checkTerms()

	// wait for starts to finish
	for i := 1; i < 2*iters; i++ {
		cfg.wait(i, servers-1, term)
	}

	_, isLeader := cfg.rafts[leader1].GetState()
	// log.Printf("isLeader: %t", isLeader)

	// submit starts to old leader before reconnecting
	// starts should fail and nothing will be committed
	for i := 0; i < iters; i++ {
		_, _, result := cfg.rafts[leader1].Start(i + iters)
		// log.Printf("leader starting: %t", result)
		time.Sleep(time.Millisecond)
	}

	cfg.connect(leader1)

	// wait for new election to occur
	time.Sleep(1000 * time.Millisecond)
	// log.Printf("new leader %d", cfg.checkOneLeader())
	time.Sleep(1000 * time.Millisecond)

	term = cfg.checkTerms()

	for i := 1; i < iters*2; i++ {
		cfg.wait(i, servers, term)
	}

	if len(cfg.logs[leader1]) > iters*2 {
		t.Fatalf("old leader committed starts while it was disconnected!")
	}

	cfg.end()
}

func TestStressPersistMe(t *testing.T) {
	servers := 10
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (3C): persist many times on one server")

	leader := cfg.checkOneLeader()
	follower := (leader + 1) % servers

	persistedTerm, _ := cfg.rafts[follower].GetState()

	iters := 500
	for i := 0; i < iters; i++ {
		for j := 0; j < iters; j++ {
			if i%7 == 0 {
				cfg.crash1(follower)
			}
			if i%8 == 0 {
				cfg.disconnect(follower)
			}
			if j%9 == 0 {
				cfg.start1(follower, cfg.applier)
			}
			if j%10 == 0 {
				cfg.connect(follower)
			}
		}
	}

	cfg.connect(follower)

	cfg.checkOneLeader()
	newTerm, _ := cfg.rafts[follower].GetState()

	// disconnecting or crashing follower should have no effect on current term, which should be persisted
	if newTerm != persistedTerm {
		t.Fatalf("terms don't match: persisted %d vs current %d", persistedTerm, newTerm)
	}

	cfg.end()
}
