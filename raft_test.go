package jsn_raft

import "testing"

func TestNewRaft(t *testing.T) {
	clusterNum = 5
	globalWg.Add(clusterNum)
	cluster := []string{
		"127.0.0.1:34591",
		"127.0.0.1:34592",
		"127.0.0.1:34593",
		"127.0.0.1:34594",
		"127.0.0.1:34595",
	}
	for _, v := range cluster {
		v := v
		go NewRaft(v, v, cluster)
	}
	globalWg.Wait()
	for {

	}
}
