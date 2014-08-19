/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2014
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Chance Zibolski (chance.zibolski@gmail.com)
#
# ***** END LICENSE BLOCK *****/

package nats

import (
	"errors"
	"github.com/mozilla-services/heka/pipeline"
	"time"

	"github.com/apcera/nats"
)

type NatsOutputConfig struct {
	Url            string   `toml:"url"`
	Servers        []string `toml:"servers"`
	NoRandomize    bool     `toml:"no_randomize"`
	Subject        string   `toml:"subject"`
	AllowReconnect bool     `toml:"reconnect"`
	MaxReconnect   int      `toml:"max_reconnects"`
	// Time in Seconds to wait before reconnecting
	ReconnectWait uint32 `toml:"reconnect_wait"`
	// Timeout in Milliseconds
	Timeout uint32 `toml:"timeout"`
}

type NatsOutput struct {
	*NatsOutputConfig
	Conn    Connection
	Options *nats.Options
	stop    chan error
}

func (output *NatsOutput) ConfigStruct() interface{} {
	return &NatsOutputConfig{Url: nats.DefaultURL}
}

func (output *NatsOutput) Init(config interface{}) error {
	conf := config.(*NatsOutputConfig)
	output.NatsOutputConfig = conf
	output.stop = make(chan error, 1)
	options := &nats.Options{
		Url:            conf.Url,
		Servers:        conf.Servers,
		NoRandomize:    conf.NoRandomize,
		AllowReconnect: conf.AllowReconnect,
		MaxReconnect:   conf.MaxReconnect,
	}

	if conf.ReconnectWait > 0 {
		options.ReconnectWait = time.Duration(conf.ReconnectWait) * time.Second
	}
	if conf.Timeout > 0 {
		options.Timeout = time.Duration(conf.Timeout) * time.Millisecond
	}
	options.ClosedCB = func(c *nats.Conn) {
		output.stop <- errors.New("Connection Closed.")
	}
	output.Options = options
	return nil
}

func (output *NatsOutput) Run(runner pipeline.OutputRunner,
	helper pipeline.PluginHelper) (err error) {
	defer close(output.stop)
	if runner.Encoder() == nil {
		return errors.New("Encoder required.")
	}

	output.Conn, err = output.Options.Connect()
	if err != nil {
		return
	}
	defer output.Conn.Close()

	var (
		pack     *pipeline.PipelinePack
		outgoing []byte
	)

	inChan := runner.InChan()
	ok := true

	for ok {
		select {
		case pack, ok = <-inChan:
			if !ok {
				return
			}
			outgoing, err = runner.Encode(pack)
			if err != nil {
				runner.LogError(err)
				continue
			}
			err = output.Conn.Publish(output.Subject, outgoing)
			if err != nil {
				runner.LogError(err)
			}
			pack.Recycle()
		case err = <-output.stop:
			return err
		}
	}

	return nil
}

func init() {
	pipeline.RegisterPlugin("NatsOutput", func() interface{} {
		return new(NatsOutput)
	})
}
