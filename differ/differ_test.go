// Copyright (c) 2018 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package differ

import (
	"crypto/sha512"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/gomemcached"
	"github.com/couchbase/xdcrDiffer/dcp"
	fdp "github.com/couchbase/xdcrDiffer/fileDescriptorPool"
	"github.com/stretchr/testify/assert"
)

const MaxUint64 = ^uint64(0)
const MinUint = 0

var randomOnce sync.Once

func randomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}
	return string(bytes)
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

// serialize mutation into []byte
// format:
//
//	keyLen  - 2 bytes
//	key  - length specified by keyLen
//	seqno   - 8 bytes
//	revId   - 8 bytes
//	cas     - 8 bytes
//	flags   - 4 bytes
//	expiry  - 4 bytes
//	opCode - 1 bytes
//	hash    - 64 bytes
func genTestData(regularMutation, colFilters bool) (key string, seqno, revId, cas uint64, flags, expiry uint32, opCode gomemcached.CommandCode, hash [64]byte, ret []byte, colId uint32, filterIds []uint8) {
	randomOnce.Do(func() {
		rand.Seed(time.Now().UTC().UnixNano())
	})

	key = randomString(randInt(12, 64))
	seqno = rand.Uint64()
	revId = rand.Uint64()
	cas = rand.Uint64()
	flags = rand.Uint32()
	expiry = rand.Uint32()
	if regularMutation {
		opCode = gomemcached.UPR_MUTATION
	} else {
		opCodeArray := [3]gomemcached.CommandCode{gomemcached.UPR_MUTATION, gomemcached.UPR_DELETION, gomemcached.UPR_EXPIRATION}
		opCode = opCodeArray[rand.Uint32()%3]
	}
	// Note we don't have the actual body hash so just randomly generate a hash using key
	hash = sha512.Sum512([]byte(key))

	if colFilters {
		randomLen := uint8(rand.Int() % 8)
		for i := uint8(0); i < randomLen; i++ {
			filterIds = append(filterIds, i)
		}
	}

	//dataSlice := createDataByteSlice(key, seqno, revId, cas, flags, expiry, opCode, hash, colId, filterIds)
	mutationToSerialize := dcp.Mutation{
		Vbno:              0,
		Key:               []byte(key),
		Seqno:             seqno,
		RevId:             revId,
		Cas:               cas,
		Flags:             flags,
		Expiry:            expiry,
		OpCode:            opCode,
		Value:             []byte(key),
		Datatype:          0,
		ColId:             0,
		ColFiltersMatched: filterIds,
	}
	dataSlice := mutationToSerialize.Serialize()

	return key, seqno, revId, cas, flags, expiry, opCode, hash, dataSlice, colId, filterIds
}

func genMultipleRecords(numOfRecords int) []byte {
	var retSlice []byte

	for i := 0; i < numOfRecords; i++ {
		_, _, _, _, _, _, _, _, record, _, _ := genTestData(true, false)
		retSlice = append(retSlice, record...)
	}

	return retSlice
}

func genSameFiles(numOfRecords int, fileName1, fileName2 string) error {
	data := genMultipleRecords(numOfRecords)

	err := ioutil.WriteFile(fileName1, data, 0644)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(fileName2, data, 0644)
	if err != nil {
		return err
	}

	return nil
}

func genMismatchedFiles(numOfRecords, mismatchCnt int, fileName1, fileName2 string) ([]string, error) {
	var mismatchedKeyNames []string
	data := genMultipleRecords(numOfRecords - mismatchCnt)

	err := ioutil.WriteFile(fileName1, data, 0644)
	if err != nil {
		return mismatchedKeyNames, err
	}

	err = ioutil.WriteFile(fileName2, data, 0644)
	if err != nil {
		return mismatchedKeyNames, err
	}

	// Now create mismatched entries
	f1, err := os.OpenFile(fileName1, os.O_APPEND|os.O_WRONLY, 644)
	if err != nil {
		return mismatchedKeyNames, err
	}
	defer f1.Close()

	f2, err := os.OpenFile(fileName2, os.O_APPEND|os.O_WRONLY, 644)
	if err != nil {
		return mismatchedKeyNames, err
	}
	defer f2.Close()

	for i := 0; i < mismatchCnt; i++ {
		key, seqno, revId, cas, flags, expiry, opCode, _, oneData, colId, _ := genTestData(true, false)
		mismatchedDataMut := &dcp.Mutation{
			Vbno:              0,
			Key:               []byte(key),
			Seqno:             seqno,
			RevId:             revId,
			Cas:               cas,
			Flags:             flags,
			Expiry:            expiry,
			OpCode:            opCode,
			Value:             []byte(key),
			Datatype:          0,
			ColId:             colId,
			ColFiltersMatched: nil,
		}
		mismatchedData := mismatchedDataMut.Serialize()

		_, err = f1.Write(oneData)
		if err != nil {
			return mismatchedKeyNames, err
		}

		_, err = f2.Write(mismatchedData)
		if err != nil {
			return mismatchedKeyNames, err
		}

		mismatchedKeyNames = append(mismatchedKeyNames, key)
	}

	return mismatchedKeyNames, nil
}

func verifyMisMatch(mismatchKeys []string, differ *FilesDiffer) bool {
	for _, key := range mismatchKeys {
		found := false
		for _, onePair := range differ.BothExistButMismatch {
			if key == onePair[0].Key {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func TestLoader(t *testing.T) {
	assert := assert.New(t)
	var outputFileTemp string = "/tmp/xdcrDiffer.tmp"
	defer os.Remove(outputFileTemp)

	key, seqno, _, _, _, _, _, _, data, _, _ := genTestData(true, false)

	err := ioutil.WriteFile(outputFileTemp, data, 0644)
	assert.Nil(err)

	differ := NewFilesDiffer(outputFileTemp, "", nil, nil, nil)
	err = differ.file1.LoadFileIntoBuffer()
	assert.Nil(err)

	assert.Equal(1, len(differ.file1.entries[0]))
	assert.Equal(seqno, differ.file1.entries[0][key].Seqno)

	assert.Equal(1, len(differ.file1.sortedEntries[0]))
	assert.Equal(seqno, differ.file1.sortedEntries[0][0].Seqno)
}

func TestLoaderWithColFilters(t *testing.T) {
	assert := assert.New(t)
	var outputFileTemp string = "/tmp/xdcrDiffer.tmp"
	defer os.Remove(outputFileTemp)

	key, _, _, _, _, _, _, _, data, _, filterIds := genTestData(true, true)

	err := ioutil.WriteFile(outputFileTemp, data, 0644)
	assert.Nil(err)

	differ := NewFilesDiffer(outputFileTemp, "", nil, nil, nil)
	err = differ.file1.LoadFileIntoBuffer()
	assert.Nil(err)

	assert.Equal(1, len(differ.file1.entries[0]))
	assert.Equal(uint8(len(filterIds)), differ.file1.entries[0][key].ColMigrFilterLen)
	for i := 0; i < len(filterIds); i++ {
		assert.Equal(filterIds[i], differ.file1.entries[0][key].ColFiltersMatched[i])
	}
}

func TestLoadSameFile(t *testing.T) {
	fmt.Println("============== Test case start: TestLoadSameFile =================")
	assert := assert.New(t)

	file1 := "/tmp/test1.bin"
	file2 := "/tmp/test2.bin"
	defer os.Remove(file1)
	defer os.Remove(file2)

	entries := 10000

	err := genSameFiles(entries, file1, file2)
	assert.Equal(nil, err)

	differ := NewFilesDiffer(file1, file2, nil, nil, nil)
	assert.NotNil(differ)

	srcDiffMap, tgtDiffMap, _, _, _ := differ.Diff()

	assert.True(len(srcDiffMap) == 0)
	assert.True(len(tgtDiffMap) == 0)
	differ.PrettyPrintResult()
	fmt.Println("============== Test case end: TestLoadSameFile =================")
}

// This test used to work because it used a customized test generator
// But now that is incorrect and the test is no longer valid
func Disabled_TestLoadMismatchedFilesOnly(t *testing.T) {
	fmt.Println("============== Test case start: TestLoadMismatchedFilesOnly =================")
	assert := assert.New(t)

	file1 := "/tmp/test1.bin"
	file2 := "/tmp/test2.bin"
	defer os.Remove(file1)
	defer os.Remove(file2)

	entries := 10000
	numMismatch := 5

	keys, err := genMismatchedFiles(entries, numMismatch, file1, file2)
	assert.Nil(err)

	differ := NewFilesDiffer(file1, file2, nil, nil, nil)
	assert.NotNil(differ)

	srcDiffMap, tgtDiffMap, _, _, _ := differ.Diff()

	assert.False(len(srcDiffMap) == 0)
	assert.False(len(tgtDiffMap) == 0)

	assert.Equal(numMismatch, len(differ.BothExistButMismatch))
	assert.True(verifyMisMatch(keys, differ))

	assert.Equal(0, len(differ.MissingFromFile1))
	assert.Equal(0, len(differ.MissingFromFile2))

	differ.PrettyPrintResult()
	fmt.Println("============== Test case end: TestLoadMismatchedFilesOnly =================")
}

// This test used to work because it used a customized test generator
// But now that is incorrect and the test is no longer valid
func Disabled_TestLoadMismatchedFilesAndUneven(t *testing.T) {
	fmt.Println("============== Test case start: TestLoadMismatchedFilesAndUneven =================")
	assert := assert.New(t)

	file1 := "/tmp/test1.bin"
	file2 := "/tmp/test2.bin"
	defer os.Remove(file1)
	defer os.Remove(file2)

	entries := 1000
	numMismatch := 5
	extraEntries := 2

	keys, err := genMismatchedFiles(entries, numMismatch, file1, file2)
	assert.Nil(err)

	// Add more records to one file
	extraSliceOfPizza := genMultipleRecords(extraEntries)
	f, err := os.OpenFile(file1, os.O_APPEND|os.O_WRONLY, 644)
	assert.Nil(err)
	_, err = f.Write(extraSliceOfPizza)
	assert.Nil(err)
	f.Close()

	differ := NewFilesDiffer(file1, file2, nil, nil, nil)
	assert.NotNil(differ)

	srcDiffMap, tgtDiffMap, _, _, _ := differ.Diff()

	assert.False(len(srcDiffMap) == 0)
	assert.False(len(tgtDiffMap) == 0)

	assert.Equal(numMismatch, len(differ.BothExistButMismatch))
	assert.True(verifyMisMatch(keys, differ))

	assert.Equal(0, len(differ.MissingFromFile1))
	assert.Equal(extraEntries, len(differ.MissingFromFile2))
	differ.PrettyPrintResult()
	fmt.Println("============== Test case start: TestLoadMismatchedFilesAndUneven =================")
}

func TestLoadSameFileWPool(t *testing.T) {
	fmt.Println("============== Test case start: TestLoadSameFileWPool =================")
	assert := assert.New(t)

	fileDescPool := fdp.NewFileDescriptorPool(50)

	file1 := "/tmp/test1.bin"
	file2 := "/tmp/test2.bin"
	defer os.Remove(file1)
	defer os.Remove(file2)

	entries := 10000

	err := genSameFiles(entries, file1, file2)
	assert.Equal(nil, err)

	differ, err := NewFilesDifferWithFDPool(file1, file2, fileDescPool, nil, nil, nil)
	assert.NotNil(differ)
	assert.Nil(err)

	srcDiffMap, tgtDiffMap, _, _, _ := differ.Diff()

	assert.True(len(srcDiffMap) == 0)
	assert.True(len(tgtDiffMap) == 0)
	fmt.Println("============== Test case end: TestLoadSameFileWPool =================")
}

func TestNoFilePool(t *testing.T) {
	fmt.Println("============== Test case start: TestNoFilePool =================")
	assert := assert.New(t)

	differDriver := NewDifferDriver("", "", "", "", 2, 2, 0, nil, nil, nil)
	assert.NotNil(differDriver)
	assert.Nil(differDriver.fileDescPool)
	fmt.Println("============== Test case end: TestNoFilePool =================")
}
