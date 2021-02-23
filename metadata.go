package lbq

type deviceMetadata struct {
	pendingRequestsCount uint64 // change to *atomiccounter.Counter
	handledRequestsCount uint64 // change to *atomiccounter.Counter
	averageResponse      uint64 // change to *atomiccounter.Counter
}
