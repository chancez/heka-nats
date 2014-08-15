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
	"code.google.com/p/go-uuid/uuid"
	"fmt"
	"github.com/apcera/nats"
	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	"time"
)

type NatsInputConfig struct {
	Url            string   `toml:"url"`
	Servers        []string `toml:"servers"`
	NoRandomize    bool     `toml:"no_randomize"`
	Subject        string   `toml:"subject"`
	AllowReconnect bool     `toml:"reconnect"`
	MaxReconnect   int      `toml:"max_reconnects"`
	// Time in Seconds to wait before reconnecting
	ReconnectWait uint32 `toml:"reconnect_wait"`
	// Timeout in Milliseconds
	Timeout     uint32 `toml:"timeout"`
	DecoderName string `toml:"decoder"`
}

type NatsInput struct {
	*NatsInputConfig
	Conn        *nats.Conn
	Options     *nats.Options
	DecoderName string `toml:"decoder"`
	decoderChan chan *pipeline.PipelinePack
	runner      pipeline.InputRunner
	stop        chan struct{}
}

func (input *NatsInput) ConfigStruct() interface{} {
	return &NatsInputConfig{
		Url: nats.DefaultURL,
	}
}

func (input *NatsInput) Init(config interface{}) error {
	conf := config.(*NatsInputConfig)
	input.stop = make(chan struct{})
	input.NatsInputConfig = conf
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
	input.Options = options
	return nil
}

func (input *NatsInput) Run(runner pipeline.InputRunner,
	helper pipeline.PluginHelper) (err error) {

	var (
		pack    *pipeline.PipelinePack
		dRunner pipeline.DecoderRunner
		ok      bool
	)

	if input.DecoderName != "" {
		if dRunner, ok = helper.DecoderRunner(input.DecoderName,
			fmt.Sprintf("%s-%s", runner.Name(), input.DecoderName)); !ok {
			return fmt.Errorf("Decoder not found: %s", input.DecoderName)
		}
		input.decoderChan = dRunner.InChan()
	}
	input.runner = runner

	hostname := helper.PipelineConfig().Hostname()
	packSupply := runner.InChan()

	input.Conn, err = input.Options.Connect()
	if err != nil {
		return
	}
	defer input.Conn.Close()

	input.Conn.Subscribe(input.Subject, func(msg *nats.Msg) {
		pack = <-packSupply
		pack.Message.SetUuid(uuid.NewRandom())
		pack.Message.SetTimestamp(time.Now().UnixNano())
		pack.Message.SetType("nats.input")
		pack.Message.SetHostname(hostname)
		pack.Message.SetPayload(string(msg.Data))
		message.NewStringField(pack.Message, "subject", msg.Subject)
		input.sendPack(pack)
	})

	// Wait for the channel to be closed
	<-input.stop

	return nil
}

func (input *NatsInput) Stop() {
	close(input.stop)
}

func (input *NatsInput) sendPack(pack *pipeline.PipelinePack) {
	if input.decoderChan != nil {
		input.decoderChan <- pack
	} else {
		input.runner.Inject(pack)
	}
}

func init() {
	pipeline.RegisterPlugin("NatsInput", func() interface{} {
		return new(NatsInput)
	})
}