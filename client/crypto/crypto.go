package crypto

// Will contain helper functions to generate public and private keys
type Keys struct {
	Public             string
	Private            string
	DictionaryPubKeys  map[string]string
	DictionaryPrivKeys map[string]string
}

func (k *Keys) New() {
	// TODO: generate public and private keys
	k.Public = "TODO"
	k.Private = "TODO"
	k.DictionaryPrivKeys = make(map[string]string)
	k.DictionaryPubKeys = make(map[string]string)
}