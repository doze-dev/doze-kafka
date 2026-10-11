package console

import (
	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/settings"
)

// Teaching lines come from data. The line under a field (its unit, range, default
// and whether the broker acts on it or only keeps it) is generated from the
// settings table (internal/settings), which DescribeConfigs and validation read
// too; a template names the record, never the words. The table is not copied here.

// SettingsSource is what the console asks of the settings table. It is the table's
// accessor and nothing else, so a test can stand in for it.
type SettingsSource interface {
	Lookup(kind settings.Kind, name string) (settings.Setting, bool)
	All(kind settings.Kind) []settings.Setting
}

type tableSource struct{}

func (tableSource) Lookup(k settings.Kind, n string) (settings.Setting, bool) {
	return settings.Lookup(k, n)
}
func (tableSource) All(k settings.Kind) []settings.Setting { return settings.All(k) }

// settingsTable is where the console reads settings from.
var settingsTable SettingsSource = tableSource{}

// teachLine is the line shown under a field, from its record: what it is, then
// its unit, range, default and whether the broker acts on it. A field with no
// record gets nothing, which a test refuses to let happen (teaching_test.go).
func teachLine(kind, name string) string { return teachWith(config.Config{}, kind, name) }

// teachWith is teachLine for a broker that is running: cfg carries what only it
// knows (the partition cap).
func teachWith(cfg config.Config, kind, name string) string {
	s, ok := settingsTable.Lookup(settings.Kind(kind), name)
	if !ok {
		return ""
	}
	return s.Say(cfg)
}

// teach is the template function: a line read through the table, for this broker.
func (c *Console) teach(kind, name string) string {
	return teachWith(config.Config{
		MaxPartitions: c.facts.MaxPartitions, DefaultPartitions: c.facts.DefaultPartitions, AutoCreateTopics: c.facts.AutoCreateTopics,
	}, kind, name)
}

// formField is how a field of a console form relates to the settings table.
type formField struct {
	// Kind and Name are the field's record. Family means the field takes one of a
	// kind's records (a compatibility level), so the kind needs at least one.
	Kind   settings.Kind
	Name   string
	Family bool
	// Not says why the field is not a setting at all (it is a record's content, a
	// filter, a choice of resource); one reason is required.
	Not string
}

// formFields is every named field of every console template. The test in
// teaching_test.go parses the templates and fails for a field that is not here.
var formFields = map[string]formField{
	// Settings: each has a record, and the template shows its teaching line.
	"partitions":  {Kind: settings.Broker, Name: "max-partitions"},
	"replication": {Kind: settings.Broker, Name: "replication-factor"},
	"add":         {Kind: settings.Broker, Name: "max-partitions"},
	"level":       {Kind: settings.Registry, Family: true},
	"offset":      {Kind: settings.Group, Name: "new-offset"},

	// Not settings: what is being written, or which thing is being looked at.
	"name":           {Not: "the name of the topic being created"},
	"topic":          {Not: "which topic of a group an action is about"},
	"partition":      {Not: "which partition of a topic a request is for"},
	"partition_ctx":  {Not: "the partition the page is showing, carried through a form"},
	"key":            {Not: "a record's key"},
	"key_enc":        {Not: "how a record's key is encoded"},
	"value":          {Not: "a record's value"},
	"value_enc":      {Not: "how a record's value is encoded"},
	"tombstone":      {Not: "whether a record's value is null"},
	"headers":        {Not: "a record's headers"},
	"acks":           {Not: "the acknowledgement asked of this one produce; a request's choice, not a setting"},
	"schema_subject": {Not: "which registered schema a value is encoded with"},
	"subject":        {Not: "the name of the subject being registered"},
	"schema":         {Not: "the text of a schema"},
	"type":           {Not: "a schema's format"},
	"references":     {Not: "a schema's references"},
	"q":              {Not: "a search term"},
	"all":            {Not: "whether a search covers every partition"},
	"jump":           {Not: "an offset to jump to"},
	"at":             {Not: "a time to jump to or trace from"},
	"id":             {Not: "a trace id, or a pasted trace header, to follow"},
	"kind":           {Not: "which kind of request the Traffic page shows"},
	"group":          {Not: "the consumer group the Traffic page is narrowed to, carried through its filter form"},
	"client":         {Not: "the client the Traffic page is narrowed to, carried through its filter form"},
	"tab":            {Not: "which tab of a page a form belongs to, so its answer is the same tab"},
}
