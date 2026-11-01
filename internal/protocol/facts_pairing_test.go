package protocol

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
)

// The inspector's "result per partition" pairs each partition a request named with the
// answer the broker gave for it. A fetch of a-0 and b-0 where b-0 was refused used to
// read "a-0: refused": the test of "is this the same topic" was inverted, so every
// answered topic matched every asked one and the last partition with the same number
// won. These tests ask about two topics with the same partition numbers, give every
// partition its own answer, and read each back, at the lowest and the highest version
// offered, as the broker hands the pair over (in memory) and as the wire carries it.

var (
	pairIDA = [16]byte{0xa, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	pairIDB = [16]byte{0xb, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
)

// pairAnswer is what the broker said about one partition in these tests.
type pairAnswer struct {
	topic  string
	id     [16]byte
	part   int32
	code   int16
	offset int64 // what the request carried for it (a fetch offset, an offset to commit).
	answer int64 // what the response carried for it (a base offset, a committed offset).
}

// pairAnswers are two topics with partitions 0 and 1 each, every one answered differently.
func pairAnswers() []pairAnswer {
	return []pairAnswer{
		{"a", pairIDA, 0, 0, 10, 100},
		{"a", pairIDA, 1, 1, 11, 101},
		{"b", pairIDB, 0, 3, 12, 102},
		{"b", pairIDB, 1, 0, 13, 103},
	}
}

// offeredRange is the lowest and highest version of an API offered under any profile.
func offeredRange(t *testing.T, key APIKey) (lo, hi int16) {
	t.Helper()
	lo, hi = -1, -1
	for _, a := range APIs {
		if a.Key != key {
			continue
		}
		for _, o := range a.Versions {
			if !o.OK {
				continue
			}
			if lo < 0 || o.Min < lo {
				lo = o.Min
			}
			if o.Max > hi {
				hi = o.Max
			}
		}
	}
	if lo < 0 {
		t.Fatalf("API %d is not offered", key)
	}
	return lo, hi
}

// overWire is the request or response as it reads after being written at its version:
// what a version does not carry (a topic's name at Fetch v13, its id before) is gone.
func overWire[T interface {
	AppendTo([]byte) []byte
	ReadFrom([]byte) error
	SetVersion(int16)
}](t *testing.T, version int16, in T, out T) T {
	t.Helper()
	in.SetVersion(version)
	out.SetVersion(version)
	if err := out.ReadFrom(in.AppendTo(nil)); err != nil {
		t.Fatalf("v%d: %v", version, err)
	}
	return out
}

// pairing is one API under test: how to ask about the partitions, how to answer them
// (in the order given, which need not be the order asked), and what the inspector
// should then say about each.
type pairing struct {
	name string
	key  APIKey
	// byID is the first version that names a topic by id alone (0: never).
	byID  int16
	build func(version int16, asked, answered []pairAnswer) (kmsg.Request, kmsg.Response)
	want  func(version int16, a pairAnswer) PartResult
}

// groupByTopic walks answers topic by topic, in first-seen order.
func groupByTopic(as []pairAnswer, topic func(a pairAnswer), part func(a pairAnswer)) {
	last := ""
	for i, a := range as {
		if i == 0 || a.topic != last {
			topic(a)
			last = a.topic
		}
		part(a)
	}
}

func pairings() []pairing {
	return []pairing{
		{name: "Produce", key: kmsg.Produce,
			build: func(_ int16, asked, answered []pairAnswer) (kmsg.Request, kmsg.Response) {
				q, r := kmsg.NewPtrProduceRequest(), kmsg.NewPtrProduceResponse()
				q.Acks = -1
				groupByTopic(asked, func(a pairAnswer) {
					tp := kmsg.NewProduceRequestTopic()
					tp.Topic = a.topic
					q.Topics = append(q.Topics, tp)
				}, func(a pairAnswer) {
					pp := kmsg.NewProduceRequestTopicPartition()
					pp.Partition, pp.Records = a.part, batchOf(int(a.part)+1)
					tp := &q.Topics[len(q.Topics)-1]
					tp.Partitions = append(tp.Partitions, pp)
				})
				groupByTopic(answered, func(a pairAnswer) {
					tp := kmsg.NewProduceResponseTopic()
					tp.Topic = a.topic
					r.Topics = append(r.Topics, tp)
				}, func(a pairAnswer) {
					pp := kmsg.NewProduceResponseTopicPartition()
					pp.Partition, pp.ErrorCode, pp.BaseOffset = a.part, a.code, a.answer
					if a.code != 0 {
						pp.BaseOffset = -1
					}
					tp := &r.Topics[len(r.Topics)-1]
					tp.Partitions = append(tp.Partitions, pp)
				})
				return q, r
			},
			want: func(_ int16, a pairAnswer) PartResult {
				p := PartResult{Topic: a.topic, Partition: a.part, Records: int(a.part) + 1, Bytes: 81, ErrorCode: a.code}
				if a.code == 0 {
					p.Offset, p.HasOffset = a.answer, true
				}
				return p
			}},
		{name: "Fetch", key: kmsg.Fetch, byID: 13,
			build: func(version int16, asked, answered []pairAnswer) (kmsg.Request, kmsg.Response) {
				q, r := kmsg.NewPtrFetchRequest(), kmsg.NewPtrFetchResponse()
				groupByTopic(asked, func(a pairAnswer) {
					tp := kmsg.NewFetchRequestTopic()
					tp.Topic, tp.TopicID = a.topic, a.id
					if version < 13 {
						tp.TopicID = [16]byte{}
					} else {
						tp.Topic = ""
					}
					q.Topics = append(q.Topics, tp)
				}, func(a pairAnswer) {
					pp := kmsg.NewFetchRequestTopicPartition()
					pp.Partition, pp.FetchOffset = a.part, a.offset
					tp := &q.Topics[len(q.Topics)-1]
					tp.Partitions = append(tp.Partitions, pp)
				})
				groupByTopic(answered, func(a pairAnswer) {
					// The broker fills in the name it resolved and echoes the id it was given.
					tp := kmsg.NewFetchResponseTopic()
					tp.Topic = a.topic
					if version >= 13 {
						tp.TopicID = a.id
					}
					r.Topics = append(r.Topics, tp)
				}, func(a pairAnswer) {
					pp := kmsg.NewFetchResponseTopicPartition()
					pp.Partition, pp.ErrorCode = a.part, a.code
					if a.code == 0 {
						pp.RecordBatches = batchOf(int(a.part) + 2)
					}
					tp := &r.Topics[len(r.Topics)-1]
					tp.Partitions = append(tp.Partitions, pp)
				})
				return q, r
			},
			want: func(_ int16, a pairAnswer) PartResult {
				p := PartResult{Topic: a.topic, Partition: a.part, Offset: a.offset, HasOffset: true, ErrorCode: a.code}
				if a.code == 0 {
					p.Records, p.Bytes = int(a.part)+2, 81
				}
				return p
			}},
		{name: "OffsetCommit", key: kmsg.OffsetCommit,
			build: func(_ int16, asked, answered []pairAnswer) (kmsg.Request, kmsg.Response) {
				q, r := kmsg.NewPtrOffsetCommitRequest(), kmsg.NewPtrOffsetCommitResponse()
				q.Group = "g"
				groupByTopic(asked, func(a pairAnswer) {
					tp := kmsg.NewOffsetCommitRequestTopic()
					tp.Topic = a.topic
					q.Topics = append(q.Topics, tp)
				}, func(a pairAnswer) {
					pp := kmsg.NewOffsetCommitRequestTopicPartition()
					pp.Partition, pp.Offset = a.part, a.offset
					tp := &q.Topics[len(q.Topics)-1]
					tp.Partitions = append(tp.Partitions, pp)
				})
				groupByTopic(answered, func(a pairAnswer) {
					tp := kmsg.NewOffsetCommitResponseTopic()
					tp.Topic = a.topic
					r.Topics = append(r.Topics, tp)
				}, func(a pairAnswer) {
					pp := kmsg.NewOffsetCommitResponseTopicPartition()
					pp.Partition, pp.ErrorCode = a.part, a.code
					tp := &r.Topics[len(r.Topics)-1]
					tp.Partitions = append(tp.Partitions, pp)
				})
				return q, r
			},
			want: func(_ int16, a pairAnswer) PartResult {
				return PartResult{Topic: a.topic, Partition: a.part, Offset: a.offset, HasOffset: true, ErrorCode: a.code}
			}},
		{name: "TxnOffsetCommit", key: kmsg.TxnOffsetCommit,
			build: func(_ int16, asked, answered []pairAnswer) (kmsg.Request, kmsg.Response) {
				q, r := kmsg.NewPtrTxnOffsetCommitRequest(), kmsg.NewPtrTxnOffsetCommitResponse()
				q.Group, q.TransactionalID = "g", "tx"
				groupByTopic(asked, func(a pairAnswer) {
					tp := kmsg.NewTxnOffsetCommitRequestTopic()
					tp.Topic = a.topic
					q.Topics = append(q.Topics, tp)
				}, func(a pairAnswer) {
					pp := kmsg.NewTxnOffsetCommitRequestTopicPartition()
					pp.Partition, pp.Offset = a.part, a.offset
					tp := &q.Topics[len(q.Topics)-1]
					tp.Partitions = append(tp.Partitions, pp)
				})
				groupByTopic(answered, func(a pairAnswer) {
					tp := kmsg.NewTxnOffsetCommitResponseTopic()
					tp.Topic = a.topic
					r.Topics = append(r.Topics, tp)
				}, func(a pairAnswer) {
					pp := kmsg.NewTxnOffsetCommitResponseTopicPartition()
					pp.Partition, pp.ErrorCode = a.part, a.code
					tp := &r.Topics[len(r.Topics)-1]
					tp.Partitions = append(tp.Partitions, pp)
				})
				return q, r
			},
			want: func(_ int16, a pairAnswer) PartResult {
				return PartResult{Topic: a.topic, Partition: a.part, Offset: a.offset, HasOffset: true, ErrorCode: a.code}
			}},
		{name: "ListOffsets", key: kmsg.ListOffsets,
			build: func(version int16, asked, answered []pairAnswer) (kmsg.Request, kmsg.Response) {
				q, r := kmsg.NewPtrListOffsetsRequest(), kmsg.NewPtrListOffsetsResponse()
				groupByTopic(asked, func(a pairAnswer) {
					tp := kmsg.NewListOffsetsRequestTopic()
					tp.Topic = a.topic
					q.Topics = append(q.Topics, tp)
				}, func(a pairAnswer) {
					pp := kmsg.NewListOffsetsRequestTopicPartition()
					pp.Partition, pp.Timestamp, pp.MaxNumOffsets = a.part, -1, 1
					tp := &q.Topics[len(q.Topics)-1]
					tp.Partitions = append(tp.Partitions, pp)
				})
				groupByTopic(answered, func(a pairAnswer) {
					tp := kmsg.NewListOffsetsResponseTopic()
					tp.Topic = a.topic
					r.Topics = append(r.Topics, tp)
				}, func(a pairAnswer) {
					pp := kmsg.NewListOffsetsResponseTopicPartition()
					pp.Partition, pp.ErrorCode = a.part, a.code
					if a.code == 0 {
						// v0 answers a list of offsets; later versions one.
						if version == 0 {
							pp.OldStyleOffsets = []int64{a.answer}
						} else {
							pp.Offset = a.answer
						}
					}
					tp := &r.Topics[len(r.Topics)-1]
					tp.Partitions = append(tp.Partitions, pp)
				})
				return q, r
			},
			want: func(_ int16, a pairAnswer) PartResult {
				p := PartResult{Topic: a.topic, Partition: a.part, ErrorCode: a.code}
				if a.code == 0 {
					p.Offset, p.HasOffset = a.answer, true
				}
				return p
			}},
		{name: "OffsetFetch", key: kmsg.OffsetFetch,
			build: func(version int16, asked, answered []pairAnswer) (kmsg.Request, kmsg.Response) {
				q, r := kmsg.NewPtrOffsetFetchRequest(), kmsg.NewPtrOffsetFetchResponse()
				if version >= 8 {
					qg, rg := kmsg.NewOffsetFetchRequestGroup(), kmsg.NewOffsetFetchResponseGroup()
					qg.Group, rg.Group = "g", "g"
					groupByTopic(asked, func(a pairAnswer) {
						tp := kmsg.NewOffsetFetchRequestGroupTopic()
						tp.Topic = a.topic
						qg.Topics = append(qg.Topics, tp)
					}, func(a pairAnswer) {
						tp := &qg.Topics[len(qg.Topics)-1]
						tp.Partitions = append(tp.Partitions, a.part)
					})
					groupByTopic(answered, func(a pairAnswer) {
						tp := kmsg.NewOffsetFetchResponseGroupTopic()
						tp.Topic = a.topic
						rg.Topics = append(rg.Topics, tp)
					}, func(a pairAnswer) {
						pp := kmsg.NewOffsetFetchResponseGroupTopicPartition()
						pp.Partition, pp.ErrorCode, pp.Offset = a.part, a.code, a.answer
						if a.code != 0 {
							pp.Offset = -1
						}
						tp := &rg.Topics[len(rg.Topics)-1]
						tp.Partitions = append(tp.Partitions, pp)
					})
					q.Groups, r.Groups = append(q.Groups, qg), append(r.Groups, rg)
					return q, r
				}
				q.Group = "g"
				groupByTopic(asked, func(a pairAnswer) {
					tp := kmsg.NewOffsetFetchRequestTopic()
					tp.Topic = a.topic
					q.Topics = append(q.Topics, tp)
				}, func(a pairAnswer) {
					tp := &q.Topics[len(q.Topics)-1]
					tp.Partitions = append(tp.Partitions, a.part)
				})
				groupByTopic(answered, func(a pairAnswer) {
					tp := kmsg.NewOffsetFetchResponseTopic()
					tp.Topic = a.topic
					r.Topics = append(r.Topics, tp)
				}, func(a pairAnswer) {
					pp := kmsg.NewOffsetFetchResponseTopicPartition()
					pp.Partition, pp.ErrorCode, pp.Offset = a.part, a.code, a.answer
					if a.code != 0 {
						pp.Offset = -1
					}
					tp := &r.Topics[len(r.Topics)-1]
					tp.Partitions = append(tp.Partitions, pp)
				})
				return q, r
			},
			want: func(_ int16, a pairAnswer) PartResult {
				p := PartResult{Topic: a.topic, Partition: a.part, ErrorCode: a.code}
				if a.code == 0 {
					p.Offset, p.HasOffset = a.answer, true
				}
				return p
			}},
		{name: "DeleteRecords", key: kmsg.DeleteRecords,
			build: func(_ int16, asked, answered []pairAnswer) (kmsg.Request, kmsg.Response) {
				q, r := kmsg.NewPtrDeleteRecordsRequest(), kmsg.NewPtrDeleteRecordsResponse()
				groupByTopic(asked, func(a pairAnswer) {
					tp := kmsg.NewDeleteRecordsRequestTopic()
					tp.Topic = a.topic
					q.Topics = append(q.Topics, tp)
				}, func(a pairAnswer) {
					pp := kmsg.NewDeleteRecordsRequestTopicPartition()
					pp.Partition, pp.Offset = a.part, a.offset
					tp := &q.Topics[len(q.Topics)-1]
					tp.Partitions = append(tp.Partitions, pp)
				})
				groupByTopic(answered, func(a pairAnswer) {
					tp := kmsg.NewDeleteRecordsResponseTopic()
					tp.Topic = a.topic
					r.Topics = append(r.Topics, tp)
				}, func(a pairAnswer) {
					pp := kmsg.NewDeleteRecordsResponseTopicPartition()
					pp.Partition, pp.ErrorCode, pp.LowWatermark = a.part, a.code, a.answer
					if a.code != 0 {
						pp.LowWatermark = -1
					}
					tp := &r.Topics[len(r.Topics)-1]
					tp.Partitions = append(tp.Partitions, pp)
				})
				return q, r
			},
			want: func(_ int16, a pairAnswer) PartResult {
				p := PartResult{Topic: a.topic, Partition: a.part, ErrorCode: a.code}
				if a.code == 0 {
					p.Offset, p.HasOffset = a.answer, true
				}
				return p
			}},
		{name: "OffsetDelete", key: kmsg.OffsetDelete,
			build: func(_ int16, asked, answered []pairAnswer) (kmsg.Request, kmsg.Response) {
				q, r := kmsg.NewPtrOffsetDeleteRequest(), kmsg.NewPtrOffsetDeleteResponse()
				q.Group = "g"
				groupByTopic(asked, func(a pairAnswer) {
					tp := kmsg.NewOffsetDeleteRequestTopic()
					tp.Topic = a.topic
					q.Topics = append(q.Topics, tp)
				}, func(a pairAnswer) {
					pp := kmsg.NewOffsetDeleteRequestTopicPartition()
					pp.Partition = a.part
					tp := &q.Topics[len(q.Topics)-1]
					tp.Partitions = append(tp.Partitions, pp)
				})
				groupByTopic(answered, func(a pairAnswer) {
					tp := kmsg.NewOffsetDeleteResponseTopic()
					tp.Topic = a.topic
					r.Topics = append(r.Topics, tp)
				}, func(a pairAnswer) {
					pp := kmsg.NewOffsetDeleteResponseTopicPartition()
					pp.Partition, pp.ErrorCode = a.part, a.code
					tp := &r.Topics[len(r.Topics)-1]
					tp.Partitions = append(tp.Partitions, pp)
				})
				return q, r
			},
			want: func(_ int16, a pairAnswer) PartResult {
				return PartResult{Topic: a.topic, Partition: a.part, ErrorCode: a.code}
			}},
	}
}

func pairNames(id [16]byte) string {
	switch id {
	case pairIDA:
		return "a"
	case pairIDB:
		return "b"
	}
	return ""
}

func TestFactsPairEachPartitionWithItsOwnAnswer(t *testing.T) {
	asked := pairAnswers()
	orders := map[string][]pairAnswer{
		"answered in the order asked": asked,
		// Topics the other way round, and each topic's partitions too.
		"answered in another order": {asked[3], asked[2], asked[1], asked[0]},
	}
	for _, pc := range pairings() {
		lo, hi := offeredRange(t, pc.key)
		for _, version := range []int16{lo, hi} {
			for order, answered := range orders {
				for _, wire := range []bool{false, true} {
					name := fmt.Sprintf("%s v%d, %s, wire=%v", pc.name, version, order, wire)
					t.Run(name, func(t *testing.T) {
						q, r := pc.build(version, asked, answered)
						q.SetVersion(version)
						r.SetVersion(version)
						if wire {
							q = overWire(t, version, q, kmsg.RequestForKey(int16(pc.key)))
							r = overWire(t, version, r, q.ResponseKind())
						}
						f := DescribeFactsNamed(q, r, pairNames)
						if len(f.Partitions) != len(asked) || f.PartitionsMore != 0 {
							t.Fatalf("%d partitions and %d more, want %d and 0: %+v", len(f.Partitions), f.PartitionsMore, len(asked), f.Partitions)
						}
						for i, a := range asked {
							if got, want := f.Partitions[i], pc.want(version, a); got != want {
								t.Errorf("%s-%d = %+v\n            want %+v", a.topic, a.part, got, want)
							}
						}
					})
				}
			}
		}
	}
}

// The reviewers' case, spelled out: a v12 fetch of a-0 and b-0 where b-0 is refused.
func TestAFetchOfTwoTopicsDoesNotGiveOneTopicsErrorToTheOther(t *testing.T) {
	q, r := kmsg.NewPtrFetchRequest(), kmsg.NewPtrFetchResponse()
	q.SetVersion(12)
	for _, name := range []string{"a", "b"} {
		tp := kmsg.NewFetchRequestTopic()
		tp.Topic = name
		tp.Partitions = append(tp.Partitions, kmsg.NewFetchRequestTopicPartition())
		q.Topics = append(q.Topics, tp)
		rt := kmsg.NewFetchResponseTopic()
		rt.Topic = name
		rp := kmsg.NewFetchResponseTopicPartition()
		if name == "b" {
			rp.ErrorCode = 3
		} else {
			rp.RecordBatches = batchOf(5)
		}
		rt.Partitions = append(rt.Partitions, rp)
		r.Topics = append(r.Topics, rt)
	}
	f := DescribeFacts(q, r)
	want := []PartResult{
		{Topic: "a", Partition: 0, HasOffset: true, Records: 5, Bytes: 81},
		{Topic: "b", Partition: 0, HasOffset: true, ErrorCode: 3},
	}
	if !reflect.DeepEqual(f.Partitions, want) {
		t.Errorf("partitions = %+v\n        want %+v", f.Partitions, want)
	}
}

// A partition the broker did not answer (a response cut short by a top-level error, an
// acks=0 produce) is listed as asked, with no result invented for it.
func TestAPartitionWithNoAnswerHasNoResult(t *testing.T) {
	for _, pc := range pairings() {
		_, hi := offeredRange(t, pc.key)
		asked := pairAnswers()
		q, r := pc.build(hi, asked, asked[:1])
		for name, resp := range map[string]kmsg.Response{"no response": nil, "one partition answered": r} {
			f := DescribeFactsNamed(q, resp, pairNames)
			if len(f.Partitions) != len(asked) {
				t.Fatalf("%s, %s: %d partitions, want %d", pc.name, name, len(f.Partitions), len(asked))
			}
			for i, p := range f.Partitions[1:] {
				if p.ErrorCode != 0 || p.Topic != asked[i+1].topic || p.Partition != asked[i+1].part {
					t.Errorf("%s, %s: unanswered %s-%d reads %+v", pc.name, name, asked[i+1].topic, asked[i+1].part, p)
				}
			}
		}
	}
}

// An OffsetFetch that names no partition asks for everything the group has committed;
// what came back is then the list.
func TestAnOffsetFetchOfEverythingListsWhatCameBack(t *testing.T) {
	for _, version := range []int16{2, 7, 8, 9} {
		pc := pairings()[5]
		_, r := pc.build(version, nil, pairAnswers())
		q, _ := pc.build(version, nil, nil)
		q.(*kmsg.OffsetFetchRequest).Topics = nil
		f := DescribeFacts(q, r)
		if len(f.Partitions) != 4 || f.Partitions[2] != (PartResult{Topic: "b", Partition: 0, ErrorCode: 3}) ||
			f.Partitions[3] != (PartResult{Topic: "b", Partition: 1, Offset: 103, HasOffset: true}) {
			t.Errorf("v%d: %+v", version, f.Partitions)
		}
	}
}

// An OffsetFetch for several groups has no one group to list partitions for, and a row
// has no column for the group: it lists none rather than mix them.
func TestAnOffsetFetchForSeveralGroupsListsNoPartitions(t *testing.T) {
	pc := pairings()[5]
	q, r := pc.build(9, pairAnswers(), pairAnswers())
	oq, or := q.(*kmsg.OffsetFetchRequest), r.(*kmsg.OffsetFetchResponse)
	oq.Groups, or.Groups = append(oq.Groups, oq.Groups[0]), append(or.Groups, or.Groups[0])
	oq.Groups[1].Group, or.Groups[1].Group = "h", "h"
	if f := DescribeFacts(q, r); len(f.Partitions) != 0 || f.PartitionsMore != 0 {
		t.Errorf("%+v, %d more", f.Partitions, f.PartitionsMore)
	}
}
