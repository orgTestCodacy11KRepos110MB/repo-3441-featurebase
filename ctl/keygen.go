// Copyright 2021 Molecula Corp. All rights reserved.
package ctl

import (
	"context"
	"fmt"
	"io"

	"github.com/gorilla/securecookie"
	pilosa "github.com/molecula/featurebase/v2"
)

// Keygen represents a command for generating a crytographic key.
type KeygenCommand struct {
	CmdIO     *pilosa.CmdIO
	KeyLength int
}

// NewKeygen returns a new instance of Keygen.
func NewKeygenCommand(stdin io.Reader, stdout, stderr io.Writer) *KeygenCommand {
	return &KeygenCommand{
		CmdIO: pilosa.NewCmdIO(stdin, stdout, stderr),
	}
}

// Run keygen to obtain key to use for authentication .
func (kg *KeygenCommand) Run(_ context.Context) error {
	fmt.Printf("secret-key = \"%+x\"\n", securecookie.GenerateRandomKey(kg.KeyLength))
	return nil
}
