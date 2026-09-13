package domain

// VerifyReconciliation records a successful, independently executed check in
// the durable run summary. Missing keys mean unverified, including skipped
// checks and runs created by older service versions. A check never supplies
// accounting evidence; drift resolvers still require attributable ledger events.
func (run *ReconciliationRun) VerifyReconciliation(kind, identity string) {
	if run.Summary == nil {
		run.Summary = make(map[string]int)
	}
	run.Summary["verified_"+kind+":"+identity] = 1
}
