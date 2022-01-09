package main

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

// URLTop10 .
func URLTop10(nWorkers int) RoundsArgs {
	// YOUR CODE HERE :)
	// And don't forget to document your idea.
	var args RoundsArgs
	// round 1: do url count
	args = append(args, RoundArgs{
		MapFunc:    URLCountMap,
		ReduceFunc: URLCountReduce,
		NReduce:    nWorkers,
	})
	// round 2: sort and get the 10 most frequent URLs
	args = append(args, RoundArgs{
		MapFunc:    URLTop10Map,
		ReduceFunc: URLTop10Reduce,
		NReduce:    1,
	})
	return args
}

func URLCountMap(filename string, contents string) []KeyValue {
	lines := strings.Split(contents, "\n")
	m := make(map[string]int) 
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if len(l) == 0 {
			continue
		}
		m[l] = m[l] + 1
	}
	kvs := make([]KeyValue, 0)
	for k, v := range m {
		kvs = append(kvs, KeyValue{Key: k, Value: strconv.Itoa(v)})
	}
	return kvs
}

func URLCountReduce(key string, values []string) string {
	var fresum = 0
	for _, fre := range values {
		intfre, err := strconv.Atoi(fre)
		if err != nil {
			panic(err)
		}
		fresum += intfre
	}
	return fmt.Sprintf("%s %s\n", key, strconv.Itoa(fresum))
}

func URLTop10Map(filename string, contents string) []KeyValue {
	// reduce sub part firt in map phase   
	lines := strings.Split(contents, "\n")
	sub_reduce_result := URLTop10Reduce("", lines)

	// map
	lines = strings.Split(sub_reduce_result, "\n")
	kvs := make([]KeyValue, 0, len(lines))
	for _, l := range lines {
		ls := strings.Split(l, ":")
		if len(ls) > 1 {
			kvs = append(kvs, KeyValue{"", ls[0] + ls[1]})
		}else {
			kvs = append(kvs, KeyValue{"", l})
		}
	}
	return kvs
}

func URLTop10Reduce(key string, values []string) string {
	cnts := make(map[string]int, len(values))
	for _, v := range values {
		v := strings.TrimSpace(v)
		if len(v) == 0 {
			continue
		}
		tmp := strings.Split(v, " ")
		n, err := strconv.Atoi(tmp[1])
		if err != nil {
			panic(err)
		}
		cnts[tmp[0]] = n
	}

	us, cs := TopN(cnts, 10)
	buf := new(bytes.Buffer)
	for i := range us {
		fmt.Fprintf(buf, "%s: %d\n", us[i], cs[i])
	}
	return buf.String()
}


