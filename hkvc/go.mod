module hkvc

go 1.26.1

require (
	raft v0.0.0
	remote v0.0.0
)

replace (
	raft => ../raft
	remote => ../remote
)
