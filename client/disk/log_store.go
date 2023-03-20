package disk

import (
	"fmt"
	"os"
	"errors"
	"example/users/client/crypto"

	"bufio"
	"encoding/binary"
)

const (
	PERSIST_DIR = "./client_output"
	PUBLIC_KEY_DIR = "/public_keys"
	DELIM = '`'
)

type DiskStore struct {
	// contains data structures to interact with disk
	publicKeyFile  *os.File
	privateKeyFile *os.File
	raftLogFile    *os.File
	raftVarFile    *os.File
	Delim          byte
}

func DiskInit(clientName string) DiskStore {
	publicKeyName := fmt.Sprintf("%s%s/%s.pub", PERSIST_DIR, PUBLIC_KEY_DIR, clientName)
	privateKeyName := fmt.Sprintf("%s/%s/%s.priv", PERSIST_DIR, clientName, clientName)
	raftLogName := fmt.Sprintf("%s/%s/%s.state", PERSIST_DIR, clientName, clientName)
	raftVarName := fmt.Sprintf("%s/%s/%s.vars", PERSIST_DIR, clientName, clientName)

	var publicKeyFile, privateKeyFile, raftLogFile, raftVarFile *os.File

	if !Exists(clientName) {
		publicKeyFile, _ = os.Create(publicKeyName)
		privateKeyFile, _ = os.Create(privateKeyName)
		raftLogFile, _ = os.Create(raftLogName)
		raftVarFile, _ = os.Create(raftVarName)
	} else {
		publicKeyFile, _ = os.OpenFile(publicKeyName, os.O_RDWR, 0600)
		privateKeyFile, _ = os.OpenFile(privateKeyName, os.O_RDWR, 0600)
		raftLogFile, _ = os.OpenFile(raftLogName, os.O_RDWR, 0600)
		raftVarFile, _ = os.OpenFile(raftVarName, os.O_RDWR, 0600)	
	}

	return newDiskStore(publicKeyFile, privateKeyFile, raftLogFile, raftVarFile)
}

// Called when client wants to get the public key of another client from the disk
func GetPublicKeyFromDisk(clientName string) ([]byte, error) {
	publicKeyName := fmt.Sprintf("%s%s/%s.pub", PERSIST_DIR, PUBLIC_KEY_DIR, clientName)
	pubKeyFile, _ := os.OpenFile(publicKeyName, os.O_RDONLY, 0600)

	defer pubKeyFile.Close()
	privReader := bufio.NewReader(pubKeyFile)
	pub, err := privReader.ReadBytes(DELIM)
	pub = pub[:len(pub) - 1]

	return pub, err
}

// Get our private key from disk
func (d *DiskStore) GetPersonalKeysFromDisk() ([]byte, error) {
	privReader := bufio.NewReader(d.privateKeyFile)
	priv, err := privReader.ReadBytes(d.Delim)
	priv = priv[:len(priv) - 1]

	return priv, err
}

// Write our keys to the disk
func (d *DiskStore) WritePersonalKeysToDisk(keys crypto.Keys) {
	_, err := d.publicKeyFile.Write(append(keys.PubKeyToByte(), byte(d.Delim)))

	if err != nil {
		fmt.Println(keys.PubKeyToByte())
		panic("Error writing personal keys to disk.")
	}
	_, err = d.privateKeyFile.Write(append(keys.PrivKeyToByte(), byte(d.Delim)))

	if err != nil {
		fmt.Println(keys.PubKeyToByte())
		panic("Error writing personal keys to disk.")
	}
}

func (d *DiskStore) GetLogFromDisk() ([]byte, error) {
	logBytes, err := os.ReadFile(d.raftLogFile.Name())

	return logBytes, err
}

func (d *DiskStore) WriteLogToDisk(log []byte) {
	d.raftLogFile.Truncate(0)
	_, err := d.raftLogFile.WriteAt(log, 0)

	if err != nil {
		fmt.Println(err.Error())
		panic("Error writing log to disk.")
	}
}

// Get currentTerm, votedFor from disk
func (d *DiskStore) GetVarsFromDisk() (int, string, error) {
	varReader := bufio.NewReader(d.raftVarFile)

	currentTermBytes, err := varReader.ReadBytes(DELIM)

	if err != nil {
		return 0, "", err
	}

	votedForBytes, err := varReader.ReadBytes(DELIM)

	if err != nil {
		return 0, "", err
	}

	currentTermBytes = currentTermBytes[:len(currentTermBytes) - 1]
	votedForBytes = votedForBytes[:len(votedForBytes) - 1]
	currentTerm := binary.LittleEndian.Uint16(currentTermBytes)
	votedFor := string(votedForBytes)

	return int(currentTerm), votedFor, nil
}

func (d *DiskStore) WriteVarsToDisk(currentTerm int, votedFor string) error {
	currentTermByte := make([]byte, 2)
	votedForByte := []byte(votedFor)
	binary.LittleEndian.PutUint16(currentTermByte, uint16(currentTerm))
	currentTermByte = append(currentTermByte, DELIM)
	_, err := d.raftVarFile.WriteAt(append(append(currentTermByte, votedForByte...), DELIM), 0)

	return err
}

func Exists(clientName string) bool {
	publicKeyName := fmt.Sprintf("%s%s/%s.pub", PERSIST_DIR, PUBLIC_KEY_DIR, clientName)
	fd, err := os.OpenFile(publicKeyName, os.O_RDONLY, 0600)

	if err != nil {
		defer fd.Close()
	}

	if errors.Is(err, os.ErrNotExist) {
		return false
	} else if err != nil {
		panic(fmt.Sprintf("oops: %s", err.Error()))
	} else {
		return true
	}
}

func newDiskStore(pub, priv, log, vars *os.File) DiskStore {
	return DiskStore {
		publicKeyFile: pub,
		privateKeyFile: priv,
		raftLogFile: log,
		raftVarFile: vars,
		Delim: DELIM,
	}
}

func (l DiskStore) CloseFDs() {
	l.publicKeyFile.Close()
	l.privateKeyFile.Close()
	l.raftLogFile.Close()
	l.raftVarFile.Close()
}
