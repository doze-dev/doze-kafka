package console

import (
	"net/url"
	"strings"
	"testing"
)

// SCH-26: a subject named for its record (RecordNameStrategy, or TopicRecordNameStrategy's
// "<topic>-<record>") is shown on the topic whose records carry its ids, found from the
// newest records; a subject named for another topic that shares the id is not.
func TestATopicShowsTheRecordNamedSubjectsItsRecordsCarry(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	const order = `{"type":"record","name":"Order","namespace":"com.acme","fields":[{"name":"id","type":"long"}]}`
	register(t, c, "com.acme.Order", order)
	register(t, c, "audit-value", order) // the same schema under a topic-named subject
	topicWith(t, c, "orders", 1, 0)
	topicWith(t, c, "empty", 1, 0)

	schemaTab := func(topic string) string {
		return readable(between(req(t, c, "GET", "/_console/topics/"+topic+"?tab=schema", nil).Body.String(), `id="topic-schemas"`, "</div>\n<div"))
	}
	if strings.Contains(schemaTab("orders"), "com.acme.Order") {
		t.Error("a topic with no records lists a record-named subject")
	}
	rec := req(t, c, "POST", "/_console/topics/orders/produce", url.Values{"partition": {"0"}, "partition_ctx": {"0"}, "value": {`{"id": 7}`}, "schema_subject": {"com.acme.Order"}})
	if rec.Code != 200 {
		t.Fatalf("produce: %d %s", rec.Code, rec.Body)
	}
	got := schemaTab("orders")
	if !strings.Contains(got, "com.acme.Order") {
		t.Errorf("the schema tab of orders lacks com.acme.Order: %s", got)
	}
	if strings.Contains(got, "audit-value") {
		t.Errorf("the schema tab lists another topic's subject that shares the id: %s", got)
	}
	if strings.Contains(schemaTab("empty"), "com.acme.Order") {
		t.Error("an empty topic lists the subject")
	}
}
